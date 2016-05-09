# -*- mode: CPerl -*-
# 
# PBDB 1.2
# --------
# 
# The purpose of this file is to test all of the /data1.2/taxa operations.
# 

use lib '../lib';

use Test::Most tests => 1;

use JSON;
use Text::CSV_XS;

use lib 't';
use Tester;


my $T = Tester->new();


# First define the values we will be using to check the taxonomy operations.
# These are representative taxa from the database.

my @TEST_NAMES = ('Dinosauria'); #('Aves'), 'Rhynchonellida', 'Arthropoda');


# Then do some initial fetches using the taxon name.  Once we get a taxon
# identifier back, we will use that to test fetching by identifier.
# 
# We check both .json and .txt responses.

my ($taxon_id, $parent_id);

subtest 'all_children' => sub {
    
 NAME:
    foreach my $base_name ( @TEST_NAMES )
    {
	my $json_result = $T->fetch_url("/data1.2/taxa/list.json?vocab=pbdb&base_name=$base_name&status=all",
					"base name '$base_name' request ok");
	
	next NAME unless $json_result;
	
	my ($response, $rs, %taxon, $not_found);
	
	eval {
	    $response = decode_json( $json_result->content );
	    $rs = $response->{records};
	};
	
	ok( ref $rs eq 'ARRAY' && @$rs, 'content decoded') or next NAME;
    	
	# Check the records one by one.
	
	my $base_no = $rs->[0]{orig_no};
	
	foreach my $r ( @$rs )
	{
	    my $orig_no = $r->{orig_no};
	    my $accepted_no = $r->{accepted_no};
	    my $parent_no = $r->{parent_no};
	    my $name = $r->{taxon_name};
	    
	    $taxon{$orig_no} = $r;
	    
	    # For each taxon that has an accepted_no value, the corresponding
	    # taxon had better also be in the result set.
	    
	    if ( defined $accepted_no && !exists $taxon{$accepted_no} )
	    {
		$not_found++;
		diag "accepted_no of '$name' ($orig_no) not found: is $accepted_no";
	    }
	    
	    # For each taxon that has a parent_no value, the corresponding
	    # taxon had better also be in the result set, unless this is
	    # the first (base) record in the result set or one of its synonyms.
	    
	    if ( defined $parent_no && !exists $taxon{$parent_no} && $accepted_no ne $base_no )
	    {
		$not_found++;
		diag "parent of '$name' ($orig_no) not found: is $parent_no";
	    }
	}
	
	if ( defined $not_found && $not_found > 1 )
	{
	    fail("bad match for base name '$base_name'");
	}
	
	else
	{
	    diag("pass for base name '$base_name'");
	}
    }
}

