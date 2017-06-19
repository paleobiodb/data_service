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

use Test::More tests => 2;

# use CoreFunction qw(connectDB configData);
# use Taxonomy;

use lib 'lib', '../lib';

use ExternalIdent qw(%IDRE);
use TaxonDefs;

use lib 't';
use Tester;
use Test::Conditions;
use Test::Selection;


# If we provided any command-line arguments, run only subtests whose names match.

choose_subtests(@ARGV);

# Start by creating an instance of the Tester class, with which to conduct the
# following tests.

my $T = Tester->new({ prefix => 'data1.2' });

my $POS_VALUE = qr{ ^ [1-9][0-9]* $ }xs;
my $AGE_VALUE = qr{ ^ [1-9][0-9]* (?: [.] [0-9]+ )? $ }xs;
my $CC2_VALUE = qr{ ^ [A-Z][A-Z] (?: , \s* [A-Z][A-Z] )* $ }xs;

# Test the combined autocomplete operation

subtest 'auto' => sub {
    
    my $NAME1 = 'cam';
    my $NAME2 = 'CAM';
    
    my (@a1) = $T->fetch_records("/combined/auto.json?name=$NAME1", "combined auto '$NAME1'");
    
    unless ( @a1 )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    cmp_ok( @a1, '>', 5, "found at least 5 records" );
    
    my (@a2) = $T->fetch_records("/combined/auto.json?name=$NAME2", "combined auto '$NAME2'");
    my (@a3) = $T->fetch_records("/combined/auto.json?name=$NAME1&limit=3", "combined auto with limit 3");
    
    cmp_ok( @a3, '==', 3, "found exactly 3 records with limit 3");
    
    my (%nam1) = $T->extract_values(\@a1, 'nam');
    my (%nam2) = $T->extract_values(\@a2, 'nam');
    
    $T->cmp_sets_ok( \%nam1, '==', \%nam2, "found same records with upper and lower case" );
    
    my $tc = Test::Conditions->new;
    $tc->expect('type int', 'type str', 'type prs', 'type txn');
    
    foreach my $a ( @a1 )
    {
	my $oid = $a->{oid};
	
	unless ( $oid )
	{
	    $tc->flag('oid', 'MISSING VALUE');
	    next;
	}
	
	my $type = substr($oid, 0, 3);
	$tc->flag("type $type");
	
	if ( $type eq 'int' )
	{
	    $tc->flag('nam', $oid) unless $a->{nam} && lc(substr($a->{nam}, 0, length($NAME1))) eq $NAME1;
	    $tc->flag('eag', $oid) unless $a->{eag} && $a->{eag} =~ $AGE_VALUE;
	    $tc->flag('lag', $oid) unless $a->{lag} && $a->{lag} =~ $AGE_VALUE;
	}
	
	elsif ( $type eq 'str' )
	{
	    $tc->flag('nam', $a->{nam}) unless $a->{nam} && lc(substr($a->{nam}, 0, length($NAME1))) eq $NAME1;
	    $tc->flag('rnk', $a->{nam}) unless $a->{rnk} eq 'group' || $a->{rnk} eq 'member' || $a->{rnk} eq 'formation';
	    $tc->flag('cc2', $a->{nam}) unless $a->{cc2} && $a->{cc2} =~ $CC2_VALUE;
	    $tc->flag('noc', $a->{nam}) unless $a->{noc} && $a->{noc} =~ $POS_VALUE;
	    $tc->flag('nco', $a->{nam}) unless $a->{nco} && $a->{nco} =~ $POS_VALUE;
	}
	
	elsif ( $type eq 'prs' )
	{
	    $tc->flag('nam', $oid) unless $a->{nam} && $a->{nam} =~ / $NAME1/i;
	}
	
	elsif ( $type eq 'txn' )
	{
	    $tc->flag('nam', $oid) unless $a->{nam} && lc(substr($a->{nam}, 0, length($NAME1))) eq $NAME1;
	    $tc->flag('rnk', $oid) unless $a->{rnk};
	    $tc->flag('htn', $oid) unless $a->{htn};
	    $tc->flag('noc', $oid) unless $a->{noc} && $a->{noc} =~ $POS_VALUE;
	}
	
	else
	{
	    $tc->flag('bad type', $oid);
	}
    }
    
    $tc->ok_all("auto complete returned proper values");
    $tc->reset_expects;
    
    my (@a4) = $T->fetch_records("/combined/auto.json?name=$NAME1&show=countries", "combined auto with countries");
    
    foreach my $a ( @a4 )
    {
	my $oid = $a->{oid};
	my $type = $oid ? substr($oid, 0, 3) : '';
	
	next unless $type eq 'str';
	
	$tc->flag('country full', $a->{nam}) unless $a->{cc2} && $a->{cc2} =~ /[a-z]/;
    }
    
    $tc->ok_all("auto complete returned country names");
};


subtest 'auto bad values' => sub {

    my ($m1) = $T->fetch_nocheck("/combined/auto.json?name=ca", "combined auto with 2 chars");
    
    $T->ok_no_records($m1, "combined auto returned no records with 2 chars");
    $T->ok_response_code($m1, '200', "got '200' response with 2 chars" );
    
    my ($m2) = $T->fetch_nocheck("/combined/auto.json?name=ca&foo=bar", "bad parameter" );
    
    $T->ok_response_code($m2, '400', "got '400' response with bad parameter" );
};
