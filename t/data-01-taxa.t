# -*- mode: CPerl -*-
# 
# PBDB 1.2
# --------
# 
# The purpose of this file is to test the integrity of the taxonomy data,
# using /data1.2/taxa/list.
# 

use strict;
use feature 'unicode_strings';

use Test::Most tests => 1;

use lib 't';
use Tester;

# Start by creating an instance of the Tester class, with which to conduct the
# following tests.

my $T = Tester->new({ prefix => 'data1.2' });





subtest 'taxa output' => sub {
    
    my $NAME_1 = 'Mammalia';
    my $OID_1 = 'txn:36651';
    
    # We have already checked common, parent, immparent, class, classext, crmod, ent, entname above.
    
    # First check basic fields + app, size, subcounts, seq
    
    my @r1a = $T->fetch_records("/taxa/list.json?base_id=$OID_1&show=app,size,subcounts,seq");
    
    my (%record, %bad_record, $base_flag);
    
    foreach my $r ( @r1a )
    {
	if ( $r->{oid} && $r->{oid} =~ /^txn:\d+$/ ) 
	{ 
	    $record{$r->{oid}} = $r;
	    
	    $base_flag = $r->{flg} if $r->{oid} eq $OID_1;
	}
	
	else
	{
	    $bad_record{oid} ||= $r->{nam};
	}
	
	$bad_record{typ} ||= $r->{nam} unless defined $r->{typ} && $r->{typ} eq 'txn';
	$bad_record{rnk} ||= $r->{nam} unless defined $r->{rnk} && $r->{rnk} =~ /^\d+$/;
	$bad_record{noc} ||= $r->{nam} unless defined $r->{noc} && $r->{noc} =~ /^\d+$/;
	$bad_record{rid} ||= $r->{nam} unless defined $r->{rid} && $r->{rid} =~ /^ref:\d+$/;
	$bad_record{ext} ||= $r->{nam} unless ! defined $r->{ext} || $r->{ext} =~ /^[01]$/;
	$bad_record{flg} ||= $r->{nam} if defined $r->{flg} && $r->{flg} =~ /[BVE]/ && $r->{oid} ne $OID_1;
    }
    
    foreach my $f ( keys %bad_record )
    {
	ok( ! $bad_record{$f}, "all records have proper value for '$f'" ) ||
	    diag("    Got: '$bad_record{$f}' with improper field value");
    }
    
    ok( $base_flag && $base_flag =~ /B/, "base record has flag 'B'" );
    
    # Now, for each record, compare its field values to those of its parent.  Ignore the base
    # record, and compare synonyms of the base to the base (using the 'acc' field).  Ignore this
    # test if we have one or more records with improper oids, which will have alreayd been flagged
    # and is much more severe.
    
    unless ( $bad_record{oid} )
    {
	%bad_record = ();
	
	foreach my $r ( @r1a )
	{
	    next if $r->{oid} eq $OID_1;
	    my $p = $record{$r->{par}} ? $record{$r->{par}} : $record{$r->{acc}};
	    
	    # Check sequence numbers against the parent record
	    
	    $bad_record{lsq} ||= $r->{nam} unless $r->{lsq} > $p->{lsq};
	    $bad_record{rsq} ||= $r->{nam} unless $r->{rsq} <= $p->{rsq};
	    
	    # Check subcounts for proper values and against the parent record
	    
	    if ( $r->{rnk} > 3 )
	    {
		$bad_record{spc} ||= $r->{nam} unless defined $r->{spc} && $r->{spc} =~ /^\d+$/ && 
		    defined $p->{spc} && $r->{spc} <= $p->{spc};
		
		if ( $r->{rnk} > 5 )
		{   
		    $bad_record{gnc} ||= $r->{nam} unless defined $r->{gnc} && $r->{gnc} =~ /^\d+$/ && 
			defined $p->{gnc} && $r->{gnc} <= $p->{gnc};
		}
		
		else
		{
		    $bad_record{gnc} ||= $r->{nam} if defined $r->{gnc} && ! $r->{tdf};
		}
	    }
	    
	    else
	    {
		$bad_record{gnc} ||= $r->{nam} if defined $r->{gnc};
		$bad_record{spc} ||= $r->{nam} if defined $r->{spc};
	    }
	    
	    # Check size and extsize for proper values and against the parent record
	    
	    $bad_record{siz} ||= $r->{nam} unless defined $r->{siz} && $r->{siz} =~ /^\d+$/ &&
		defined $p->{siz} && $r->{siz} <= $p->{siz};
	    $bad_record{exs} ||= $r->{nam} unless defined $r->{exs} && $r->{exs} =~ /^\d+$/ &&
		defined $p->{exs} && $r->{exs} <= $p->{exs};
	    
	    $bad_record{exs} ||= $r->{nam} if $r->{ext} && $r->{exs} eq '0' || $r->{tdf};
	    $bad_record{exs} ||= $r->{nam} if defined $r->{ext} && $r->{ext} eq '0' && $r->{exs} ne '0';
	    
	    # Check first and last appearances for proper values and against the parent record,
	    # unless n_occs is zero in which case there should be no values for those fields.
	    
	    if ( $r->{noc} )
	    {
		$bad_record{noc} ||= $r->{nam} unless defined $p->{noc} && $p->{noc} >= $r->{noc};
		
		$bad_record{fea} ||= $r->{nam} unless defined $r->{fea} && $r->{fea} =~ /^\d+(?:[.]\d+)?$/ &&
		    defined $p->{fea} && $p->{fea} >= $r->{fea};
		$bad_record{fla} ||= $r->{nam} unless defined $r->{fla} && $r->{fla} =~ /^\d+(?:[.]\d+)?$/ &&
		    defined $r->{fea} && $r->{fea} > $r->{fla};
		
		$bad_record{lla} ||= $r->{nam} unless defined $r->{lla} && $r->{lla} =~ /^\d+(?:[.]\d+)?$/ &&
		    defined $p->{lla} && $p->{lla} <= $r->{lla};
		$bad_record{lea} ||= $r->{nam} unless defined $r->{lea} && $r->{lea} =~ /^\d+(?:[.]\d+)?$/ &&
		    defined $r->{lla} && $r->{lea} > $r->{lla};
	    }
	    
	    else
	    {
		# $bad_record{fea} ||= $r->{nam} if defined $r->{fea} && $r->{fea} ne '';
		# $bad_record{fla} ||= $r->{nam} if defined $r->{fea} && $r->{fla} ne '';
		# $bad_record{lea} ||= $r->{nam} if defined $r->{fea} && $r->{lea} ne '';
		# $bad_record{lla} ||= $r->{nam} if defined $r->{fea} && $r->{lla} ne '';
	    }
	}
	
	foreach my $f ( keys %bad_record )
	{
	    ok( ! $bad_record{$f}, "all records have proper value for '$f'" ) ||
		diag("    Got: '$bad_record{$f}' with improper field value");
	}
    }
    
    else
    {
	diag("    Got: '$bad_record{oid}' with improper oid");
    }
}
