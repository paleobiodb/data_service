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


my $T = Tester->new({ prefix => 'data1.1' });


# First define the values we will be using to check the taxonomy operations.
# These are representative taxa from the database.

my @TEST_NAMES = ('Graptolithina', 'Neograptina', 'Asaphidae');


# Then do some initial fetches using the taxon name.  Once we get a taxon
# identifier back, we will use that to test fetching by identifier.
# 
# We check both .json and .txt responses.

my ($taxon_id, $parent_id);

subtest 'all_children' => sub {
    
 NAME:
    foreach my $base_name ( @TEST_NAMES )
    {
	my $json_result = $T->fetch_url("taxa/list.json?vocab=pbdb&base_name=$base_name&status=all&limit=999999",
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
	
	# First record every orig_no in the result set.
	
	foreach my $r ( @$rs )
	{
	    $taxon{$r->{orig_no}} = $r;
	}
	
	# Then check senior_no and parent_no links.
	
	foreach my $r ( @$rs )
	{
	    my $orig_no = $r->{orig_no};
	    my $parent_no = $r->{parent_no};
	    my $senior_no = $r->{senior_no};
	    my $name = $r->{taxon_name};
	    
	    # For each taxon that has a senior_no value, the corresponding
	    # taxon had better also be in the result set.
	    
	    if ( defined $senior_no && !exists $taxon{$senior_no} )
	    {
		$not_found++;
		diag "senior of '$name' ($orig_no) not found: is $senior_no";
	    }
	    
	    # For each taxon that has a parent_no value, the corresponding
	    # taxon had better also be in the result set, unless this is
	    # the first (base) record in the result set.
	    
	    if ( defined $parent_no && !exists $taxon{$parent_no} && $orig_no ne $base_no )
	    {
		$not_found++;
		diag "parent of '$name' ($orig_no) not found: is $parent_no";
	    }
	}
	
	if ( defined $not_found && $not_found > 0 )
	{
	    fail("bad match for base name '$base_name'");
	}
	
	else
	{
	    diag("pass for base name '$base_name'");
	}
    }
}

