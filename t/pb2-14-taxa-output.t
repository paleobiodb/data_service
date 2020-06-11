# -*- mode: CPerl -*-
# 
# PBDB 1.2
# --------
# 
# The purpose of this file is to test the /data1.2/taxa/list operation,
# specifically the output fields and blocks.
# 

use strict;
use feature 'unicode_strings';

use Test::Most tests => 4;

use lib 't';

use Tester;
use WDSGrabber;
use Test::Conditions;
use Test::Selection;


choose_subtests(@ARGV);

# Start by creating an instance of the Tester class, with which to conduct the
# following tests.

my $T = Tester->new({ prefix => 'data1.2' });


# Also create an instance of WDSGrabber, with which to gather information from PB2::TaxonData and
# the other modules on which it depends.

my $W = WDSGrabber->new('1.2');

$W->process_modules( qw(PB2::CommonData PB2::ReferenceData PB2::IntervalData PB2::TaxonData) );



# We already tested the basic output blocks in pb2-10-taxa-single.t, so now
# we test that 'taxa/list' produces the same records for the same names. Also test that a bad
# value for 'show' gets a proper warning.

subtest 'single and list' => sub {
    
    select_subtest || return;
    
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


# Now we go back and test that all of the documented output vocabulary fields are actually
# produced, in both the 'com' and 'pbdb' vocabularies. This is organized into several different
# subtests, just so none of them will be overly long.

my (@TEST_NAMES) = qw( Dascillidae Meandrosmilia Therezinosaurus Carnivora );

my %extra_block = ( basic => '1.2:taxa:basic' );
my %extra_skip = ( basic => 'typ,ctn,container_no' );

my %value_test = (
	'basic:com' =>  { oid => 'extid:TXN',
			  vid  => 'extid:VAR',
			  acc => 'extid:TXN',
			  par => 'extid:TXN',
			  ipn => 'extid:TXN',
			  rid => 'extid:REF',
			  rnk => 'posint',
			  ext => 'boolean',
			  noc => 'poszeroint',
			  pby => 'posint',
			  nam => 'taxonname',
			  acn => 'taxonname',
			  prl => 'taxonname',
			  ipl => 'taxonname',
			  _nonempty => 'oid,nam,rnk,par,prl,rid,pby,aut' },
	
	'basic:pbdb' => { orig_no => 'posint',
			  taxon_no => 'posint',
			  accepted_no => 'posint',
			  parent_no => 'posint',
			  immpar_no => 'posint',
			  reference_no => 'posint',
			  ref_pubyr => 'posint',
			  n_occs => 'poszeroint',
			  is_extant => \&test_extant,
			  taxon_name => 'taxonname',
			  accepted_name => 'taxonname',
			  parent_name => 'taxonname',
			  immpar_name => 'taxonname',
			  _nonempty => 'orig_no,taxon_no,taxon_name,taxon_rank,accepted_no,parent_no,parent_name,reference_no,ref_pubyr,ref_author' },
	
	'app:com' => { fea => 'poszeronum',
		       fla => 'poszeronum',
		       lea => 'poszeronum',
		       lla => 'poszeronum',
		       _record => \&test_app_fields },
	
	'app:pbdb' => { firstapp_max_ma => 'poszeronum',
			firstapp_min_ma => 'poszeronum',
			lastapp_max_ma => 'poszeronum',
			lastapp_min_ma => 'poszeronum',
		        _record => \&test_app_fields },
	
	'size:com' => { siz => 'poszeronum',
			exs => 'poszeronum' },
	
	'size:pbdb' => { taxon_size => 'poszeronum',
			 extant_size => 'poszeronum' },
	
	'subcounts:com' => { odc => 'poszeroint',
			     fmc => 'poszeroint',
			     gnc => 'poszeroint',
			     spc => 'poszeroint' },
	
	'subcounts:pbdb' => { n_orders => 'poszeroint',
			      n_families => 'poszeroint',
			      n_genera => 'poszeroint',
			      n_species => 'poszeroint' },
	
	'ent:com' => { ati => 'extid:PRS',
		       eni => 'extid:PRS',
		       mdi => 'extid:PRS',
		       _nonempty => 'ati,eni' },
	
	'ent:pbdb' => { authorizer_no => 'posint',
			enterer_no => 'posint',
			modifier_no => 'poszeroint',
			_nonempty => 'authorizer_no,enterer_no' },

	'crmod:com' => { dcr => 'datetime',
			 dmd => 'datetime',
			 _nonempty => 'dcr,dmd' },
	
	'crmod:pbdb' => { created => 'datetime',
			  modified => 'datetime',
			  _nonempty => 'created,modified' },

	);


subtest 'taxa vocab' => sub {

    select_subtest || return;
    
    # First test the basic output block, in both vocabularies.

    my @com_fields = $W->list_fields('1.2:taxa:basic', 'com', { skip => 'typ,ctn' });
    my @pbdb_fields = $W->list_fields('1.2:taxa:basic', 'pbdb', { skip => 'container_no' });
    
    my $extra = join(',', $W->list_if_blocks('1.2:taxa:basic'));
    
    my @com_urls = $T->generate_from_pattern("/taxa/list.json?base_name=@@&show=$extra", @TEST_NAMES);
    my @pbdb_urls =  $T->generate_from_pattern("/taxa/list.txt?base_name=@@&show=$extra", @TEST_NAMES);
    
    $T->test_block_output('1.2:taxa:basic', \@com_urls, "basic (1.2:taxa:basic) com",
			  \@com_fields, 'oid', $value_test{'basic:com'});
    
    $T->test_block_output('1.2:taxa:basic', \@pbdb_urls, "basic (1.2:taxa:basic) pbdb",
			  \@pbdb_fields, 'orig_no', $value_test{'basic:pbdb'});
    
    # Now test the full list of output blocks defined for taxa, with the following exceptions:
    
    my @skip_list = ('full', 'class', 'classext', 'nav', 'acconly', 'pres');
    
    my @block_keys = $W->list_values('1.2:taxa:mult_output_map', { skip => \@skip_list });

  KEY:
    foreach my $k ( @block_keys )
    {
	my $block = $W->map_lookup('1.2:taxa:mult_output_map', $k);
	
	next unless $block;
	
	@com_fields = $W->list_fields($block, 'com');
	@pbdb_fields = $W->list_fields($block, 'pbdb');
	
	my $extra = join(',', $k, $W->list_if_blocks($block));
	
	my @com_urls = $T->generate_from_pattern("/taxa/list.json?base_name=@@&show=$extra", @TEST_NAMES);
	my @pbdb_urls =  $T->generate_from_pattern("/taxa/list.txt?base_name=@@&show=$extra", @TEST_NAMES);
 	
	$T->test_block_output($block, \@com_urls, "$k ($block) com",
			      \@com_fields, 'oid', $value_test{"$k:com"});
	
	$T->test_block_output($block, \@pbdb_urls, "$k ($block) pbdb",
			      \@pbdb_fields, 'orig_no', $value_test{"$k:pbdb"});
    }
    
    ok( ! $T->test_field_value('Abc (Def) ghi jkl', 'taxonname'), "subgenus is okay" );
};


sub test_extant {

    return unless defined $_[0];
    return 'extant or extinct' if $_[0] eq '_label';
    return ($_[0] eq 'extant' || $_[0] eq 'extinct');
}


sub test_app_fields {
    
    my ($r) = @_;
    
    if ( $r eq '_label' )
    {
	return 'app fields';
    }
    
    elsif ( $r->{noc} )
    {
	return defined $r->{fea} && $r->{fea} ne '' &&
	    defined $r->{fla} && $r->{fla} ne '' &&
	    defined $r->{lea} && $r->{lea} ne '' &&
	    defined $r->{lla} && $r->{lla} ne '' &&
	    defined $r->{tei} && $r->{tei} ne '';
    }
    
    elsif ( $r->{n_occs} )
    {
	return defined $r->{firstapp_max_ma} && $r->{firstapp_max_ma} ne '' &&
	    defined $r->{firstapp_min_ma} && $r->{firstapp_min_ma} ne '' &&
	    defined $r->{lastapp_max_ma} && $r->{lastapp_max_ma} ne '' &&
	    defined $r->{lastapp_min_ma} && $r->{lastapp_min_ma} ne '' &&
	    defined $r->{early_interval} && $r->{early_interval} ne '';
    }
    
    else
    {
	return 1;
    }
}


# Now check that opinion single results match list results both for json and txt formats.

subtest 'opinion single and list' => sub {
    
    select_subtest || return;
    
    my $NAME1 = 'Dascillidae';

    # First check JSON.
    
    my @o1 = $T->fetch_records("/taxa/opinions.json?name=$NAME1&show=full", "list json a");
    
    unless ( @o1 )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    foreach my $r ( @o1 )
    {
	my $oid = $r->{oid};

	my ($single) = $T->fetch_records("/opinions/single.json?id=$oid&show=full", "single json '$oid'");

	if ( $single )
	{
	    is_deeply( $single, $r, "single json '$oid' matches list record" );
	}
    }
    
    # Then check txt.
    
    my @o2 = $T->fetch_records("/taxa/opinions.txt?name=$NAME1&show=full", "list txt a");
    
    unless ( @o2 )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    foreach my $r ( @o2 )
    {
	my $oid = $r->{opinion_no};

	my ($single) = $T->fetch_records("/opinions/single.txt?id=$oid&show=full", "single txt '$oid'");

	if ( $single )
	{
	    is_deeply( $single, $r, "single json '$oid' matches list record" );
	}
    }
};


my (@OPINION_NAMES) = qw( Dascillidae Meandrosmilia Therezinosaurus Felis );

my %opinion_test = (
	'basic:com' => { oid => 'extid:OPN' },
	
	'basic:pbdb' => { opinion_no => 'posint' },
	
	'ent:com' => { ati => 'extid:PRS',
		       eni => 'extid:PRS',
		       mdi => 'extid:PRS',
		       _nonempty => 'ati,eni' },
	
	'ent:pbdb' => { authorizer_no => 'posint',
			enterer_no => 'posint',
			modifier_no => 'poszeroint',
			_nonempty => 'authorizer_no,enterer_no' },
	
	'crmod:com' => { dcr => 'datetime',
			 dmd => 'datetime',
			 _nonempty => 'dcr,dmd' },
	
	'crmod:pbdb' => { created => 'datetime',
			  modified => 'datetime',
			  _nonempty => 'created,modified' },
	
	);


subtest 'opinion vocab' => sub {

    select_subtest || return;

    # First test the basic output block, in both vocabularies.

    my @com_fields = $W->list_fields('1.2:opinions:basic', 'com', { skip => 'typ' });
    my @pbdb_fields = $W->list_fields('1.2:opinions:basic', 'pbdb', { skip => '' });
    
    my $extra = join(',', $W->list_if_blocks('1.2:opinions:basic'));
    
    my @com_urls = $T->generate_from_pattern("/taxa/opinions.json?base_name=@@&show=$extra", @OPINION_NAMES);
    my @pbdb_urls =  $T->generate_from_pattern("/taxa/opinions.txt?base_name=@@&show=$extra", @OPINION_NAMES);
    
    $T->test_block_output('1.2:opinions:basic', \@com_urls, "basic (1.2:opinions:basic) com",
			  \@com_fields, 'oid', $opinion_test{'basic:com'});
    
    $T->test_block_output('1.2:opinions:basic', \@pbdb_urls, "basic (1.2:opinions:basic) pbdb",
			  \@pbdb_fields, 'opinion_no', $opinion_test{'basic:pbdb'});
    
    
    # Now test the full list of output blocks defined for taxa, with the following exceptions:
    
    my @skip_list = ('full');
    
    my @block_keys = $W->list_values('1.2:opinions:output_map', { skip => \@skip_list });

  KEY:
    foreach my $k ( @block_keys )
    {
	my $block = $W->map_lookup('1.2:opinions:output_map', $k);

	next unless $block;
	
	@com_fields = $W->list_fields($block, 'com');
	@pbdb_fields = $W->list_fields($block, 'pbdb');
	
	my $extra = join(',', $k, $W->list_if_blocks($block));
	
	my @com_urls = $T->generate_from_pattern("/taxa/opinions.json?base_name=@@&show=$extra", @TEST_NAMES);
	my @pbdb_urls =  $T->generate_from_pattern("/taxa/opinions.txt?base_name=@@&show=$extra", @TEST_NAMES);
 	
	$T->test_block_output($block, \@com_urls, "$k ($block) com",
			      \@com_fields, 'oid', $value_test{"$k:com"});
	
	$T->test_block_output($block, \@pbdb_urls, "$k ($block) pbdb",
			      \@pbdb_fields, 'opinion_no', $value_test{"$k:pbdb"});
    }
};

#     my @skip_list = ('full', 'ent', 'entname', 'crmod', 'class', 'classext', 'nav',
# 		     'parent', 'immparent', 'acconly', 'pres', 'common');
    
#     my @block_keys = $W->list_values('1.2:taxa:mult_output_map', { skip => \@skip_list });
#     unshift @block_keys, 'basic';
    
    
#     return unless @block_keys;
    
#  KEY:
#     foreach my $k ( @block_keys )
#     {
# 	my ($block, $show);
# 	my $field_options = { };

# 	my $tc = Test::Conditions->new;
	
# 	if ( $extra_block{$k} )
# 	{
# 	    $block = $extra_block{$k};
# 	    $show = $extra_show{$k};
# 	    $field_options->{skip} = $extra_skip{$k} if $extra_skip{$k};
# 	}
	
# 	else
# 	{
# 	    $block = $W->map_lookup('1.2:taxa:mult_output_map', $k);
# 	    $show = $k;
# 	}
	
# 	unless ( $block )
# 	{
# 	    diag "could not find block '$k'";
# 	    next KEY;
# 	}
	
# 	my @com_fields = $W->list_fields($block, $k, 'com', $field_options);
# 	my @pbdb_fields = $W->list_fields($block, $k, 'pbdb', $field_options);
# 	my @extra_shows = $W->list_if_blocks($block);
	
# 	$show .= ',' . join(',', @extra_shows) if @extra_shows;
	
# 	my %not_found = map { $_ => 1 } @com_fields, @pbdb_fields;

# 	# diag("Testing fields: " . join(',', @com_fields, @pbdb_fields));
	
#     TAXON:
# 	foreach my $taxon ( @TEST_NAMES )
# 	{
# 	    # diag "testing output block '$k' with taxon $taxon";
	    
# 	    my (@r1) = $T->fetch_records("/taxa/list.json?base_name=$taxon&show=$show", "fetched subtree '$taxon' json");
	    
# 	    next TAXON unless @r1;
	    
# 	    foreach my $r (@r1)
# 	    {
# 		foreach my $f ( %$r )
# 		{
# 		    next unless $not_found{$f};
# 		    delete $not_found{$f} if defined $r->{$f} && $r->{$f} ne '';
		    
# 		    my $check_key = "$k:com";
		    
# 		    if ( my $test_expr = $value_test{$check_key}{$f} )
# 		    {
# 			my $result = $T->test_value($r->{$f}, $test_expr);
# 			$tc->flag($f, "$r->{oid} : $result") if $result;
# 		    }
# 		}
		
# 		last TAXON unless %not_found;
# 	    }
	    
# 	    my (@r2) = $T->fetch_records("/taxa/list.txt?base_name=$taxon&show=$show", "fetched subtree '$taxon' txt");
	    
# 	    next TAXON unless @r2;
	    
# 	    foreach my $r (@r2)
# 	    {
# 		foreach my $f ( %$r )
# 		{
# 		    next unless $not_found{$f};
# 		    delete $not_found{$f} if defined $r->{$f} && $r->{$f} ne '';
		    
# 		    my $check_key = "$k:pbdb";
		    
# 		    if ( my $test_expr = $value_test{$check_key}{$f} )
# 		    {
# 			my $result = $T->test_value($r->{$f}, $test_expr);
# 			$tc->flag($f, "$r->{oid} : $result") if $result;
# 		    }
# 		}
		
# 		last TAXON unless %not_found;
# 	    }
# 	}
	
# 	my @not_found = grep { $not_found{$_} } @com_fields, @pbdb_fields;
	
# 	if ( @not_found )
# 	{
# 	    my $list = join("', '", @not_found);
# 	    fail("found output fields '$list' for block '$k'");
# 	}
	
# 	else
# 	{
# 	    pass("found all output fields for block '$k'");
# 	}

# 	$tc->ok_all("value checks passed for block '$k'")
#     }    
# };



# Now we test all of the output blocks that haven't been tested above, to make
# sure they produce the proper results under the compact vocabulary.

# subtest 'more output blocks 1' => sub {
    
#     select_subtest || return;
    
#     my $NAME_1 = 'Marsupialia';
#     my $OID_1 = 'txn:39937';
    
#     # We have already checked common, parent, immparent, class, classext, crmod, ent, entname in
#     # pb2-11-taxa-list.t.
    
#     # First check basic fields + app, size, subcounts, seq
    
#     my @r1a = $T->fetch_records("/taxa/list.json?base_id=$OID_1&show=app,size,subcounts,seq",
# 				"output blocks 1 com");
    
#     unless ( @r1a )
#     {
# 	diag("skipping remainder of subtest");
# 	return;
#     }
    
#     my (%record, $base_flag);
#     my $tc = Test::Conditions->new;
    
#     foreach my $r ( @r1a )
#     {
# 	if ( $r->{oid} && $r->{oid} =~ /^txn:\d+$/ ) 
# 	{ 
# 	    $record{$r->{oid}} = $r;
	    
# 	    $base_flag = $r->{flg} if $r->{oid} eq $OID_1;
# 	}
	
# 	else
# 	{
# 	    $tc->flag('oid', $r->{nam});
# 	}
	
# 	# $tc->flag('typ', $r->{nam}) unless defined $r->{typ} && $r->{typ} eq 'txn';
# 	$tc->flag('rnk', $r->{nam}) unless defined $r->{rnk} && $r->{rnk} =~ /^\d+$/;
# 	$tc->flag('noc', $r->{nam}) unless defined $r->{noc} && $r->{noc} =~ /^\d+$/;
# 	$tc->flag('rid', $r->{nam}) unless defined $r->{rid} && $r->{rid} =~ /^ref:\d+$/;
# 	$tc->flag('ext', $r->{nam}) unless ! defined $r->{ext} || $r->{ext} =~ /^[01]$/;
# 	$tc->flag('flg', $r->{nam}) if defined $r->{flg} && $r->{flg} =~ /[BVE]/ && $r->{oid} ne $OID_1;
#     }
    
#     $tc->ok_all("all records have proper values for basic fields");
    
#     ok( $base_flag && $base_flag =~ /B/, "base record has flag 'B'" );
    
#     # Now, for each record, compare its field values to those of its parent.  Ignore the base
#     # record, and compare synonyms of the base to the base (using the 'acc' field).  Ignore this
#     # test if we have one or more records with improper oids, which will have alreayd been flagged
#     # and is much more severe.
    
#     $tc->limit_max('fea' => 20, 'fla' => 20, 'lea' => 20, 'lla' => 20);
    
#     unless ( $tc->is_set('oid') )
#     {
# 	foreach my $r ( @r1a )
# 	{
# 	    next if $r->{oid} eq $OID_1;
# 	    my $p = $record{$r->{par}} ? $record{$r->{par}} : $record{$r->{acc}};
	    
# 	    # Check sequence numbers against the parent record
	    
# 	    $tc->flag('lsq', $r->{nam}) unless $r->{lsq} > $p->{lsq};
# 	    $tc->flag('rsq', $r->{nam}) unless $r->{rsq} <= $p->{rsq};
	    
# 	    # Check subcounts for proper values and against the parent record
	    
# 	    if ( $r->{rnk} > 3 )
# 	    {
# 		$tc->flag('spc', $r->{nam}) unless defined $r->{spc} && $r->{spc} =~ /^\d+$/;
# 		    # && defined $p->{spc} && $r->{spc} <= $p->{spc};
		
# 		if ( $r->{rnk} > 5 )
# 		{   
# 		    $tc->flag('gnc', $r->{nam}) unless defined $r->{gnc} && $r->{gnc} =~ /^\d+$/;
# 			# && defined $p->{gnc} && $r->{gnc} <= $p->{gnc};
# 		}
		
# 		else
# 		{
# 		    $tc->flag('gnc', $r->{nam}) if defined $r->{gnc} && ! $r->{tdf};
# 		}
# 	    }
	    
# 	    else
# 	    {
# 		$tc->flag('gnc', $r->{nam}) if defined $r->{gnc};
# 		$tc->flag('spc', $r->{nam}) if defined $r->{spc};
# 	    }
	    
# 	    # Check size and extsize for proper values and against the parent record
	    
# 	    $tc->flag('siz', $r->{nam}) unless defined $r->{siz} && $r->{siz} =~ /^\d+$/;
# 		# && defined $p->{siz} && $r->{siz} <= $p->{siz};
# 	    $tc->flag('exs', $r->{nam}) unless defined $r->{exs} && $r->{exs} =~ /^\d+$/;
# 	        # && defined $p->{exs} && $r->{exs} <= $p->{exs};
	    
# 	    # $tc->flag('exs', $r->{nam}) if $r->{ext} && $r->{exs} eq '0' && ! $r->{tdf};
# 	    # $tc->flag('exs', $r->{nam}) if defined $r->{ext} && $r->{ext} eq '0' && $r->{exs} ne '0';
	    
# 	    # Check first and last appearances for proper values and against the parent record,
# 	    # unless n_occs is zero in which case there should be no values for those fields.
	    
# 	    if ( $r->{noc} )
# 	    {
# 		# $tc->flag('noc', $r->{nam}) unless defined $p->{noc} && $p->{noc} >= $r->{noc};
		
# 		$tc->flag('fea', $r->{nam}) unless defined $r->{fea} && $r->{fea} =~ /^\d+(?:[.]\d+)?$/;
# 		    # && defined $p->{fea} && $p->{fea} >= $r->{fea};
# 		$tc->flag('fla', $r->{nam}) unless defined $r->{fla} && $r->{fla} =~ /^\d+(?:[.]\d+)?$/;
# 		    # && defined $r->{fea} && $r->{fea} > $r->{fla};
		
# 		$tc->flag('lla', $r->{nam}) unless defined $r->{lla} && $r->{lla} =~ /^\d+(?:[.]\d+)?$/;
# 		    # && defined $p->{lla} && $p->{lla} <= $r->{lla};
# 		$tc->flag('lea', $r->{nam}) unless defined $r->{lea} && $r->{lea} =~ /^\d+(?:[.]\d+)?$/;
# 		    # && defined $r->{lla} && $r->{lea} > $r->{lla};
# 	    }
	    
# 	    else
# 	    {
# 		# $tc->flag('fea', $r->{nam}) if defined $r->{fea} && $r->{fea} ne '';
# 		# $tc->flag('fla', $r->{nam}) if defined $r->{fea} && $r->{fla} ne '';
# 		# $tc->flag('lea', $r->{nam}) if defined $r->{fea} && $r->{lea} ne '';
# 		# $tc->flag('lla', $r->{nam}) if defined $r->{fea} && $r->{lla} ne '';
# 	    }
# 	}

# 	$tc->ok_all("all records have proper value for blocks 'app', 'size', 'subcounts', 'seq'" );
#     }
# };


# Now test the rest of the output blocks to make sure they produce the proper
# results under the compact vocabulary.

# subtest 'output blocks 2 com' => sub {
    
#     select_subtest || return;
    
#     my $NAME_1 = 'Mammalia';
#     my $OID_1 = 'txn:36651';
    
#     # Test 'attr,img,ref,refattr'
    
#     my @r2a = $T->fetch_records("/taxa/list.json?base_id=$OID_1&show=img,attr,ref,refattr",
# 				"output blocks 2 json");
    
#     unless ( @r2a )
#     {
# 	diag("skipping remainder of subtest");
# 	return;
#     }
    
#     my $tc = Test::Conditions->new;
#     $tc->limit_max( DEFAULT => 10, att => 1000 );
    
#     foreach my $r ( @r2a )
#     {
# 	$tc->flag('img', $r->{nam}) unless $r->{img} && $r->{img} =~ /^php:\d+$/;
	
# 	$tc->flag('att', $r->{nam}) unless $r->{att} && $r->{att} =~ /\w\w/ && $r->{att} =~ /\d\d\d\d/;
	
# 	$tc->flag('pby', $r->{nam}) unless $r->{pby} && $r->{pby} =~ /^\d\d\d\d$/;
	
# 	$tc->flag('aut', $r->{nam}) unless $r->{aut} && $r->{aut} =~ /\w\w/;
	
# 	$tc->flag('ref', $r->{nam}) unless $r->{ref} && $r->{ref} =~ /\w\w\w\w/ && $r->{ref} =~ /\d\d\d\d/;
#     }

#     $tc->ok_all("all records have proper value for 'img', 'attr', 'ref', 'refattr'");
# };


# The subtests above test the output blocks 'basic', 'attr', 'app', 'size',
# 'class', 'common'.

# Now we need to test the rest of the output blocks available for taxa:
# 'classext', 'parent', 'immparent', 'seq', 'img', 'ref', 'refattr', 'crmod',
# 'subcounts', 'ecospace', 'taphonomy', 'etbasis', 'ent', 'entname'

# subtest 'other output blocks' => sub {
    
#     select_subtest || return;
    
#     my $OB_NAME_1 = "Dascillus elongatus";
#     my $OB_BLOCKS_1 = "classext,parent,seq,img,ref,refattr,crmod";
    
#     my $OB_NAME_2 = "Carnivora";
#     my $OB_BLOCKS_2 = "subcounts,ecospace,taphonomy,etbasis,ent,entname";
    
# my $ob_j1 = { 'par' => 'txn:71894',
# 	      'prl' => 'Dascillus',
# 	      'phl' => 'Arthropoda',
# 	      'phn' => 'txn:18891',
# 	      'cll' => 'Insecta',
# 	      'cln' => 'txn:56637',
# 	      'odl' => 'Coleoptera',
# 	      'odn' => 'txn:69148',
# 	      'fml' => 'Dascillidae',
# 	      'fmn' => 'txn:69296',
# 	      'gnl' => 'Dascillus',
# 	      'gnn' => 'txn:71894',
# 	      'lsq' => '!pos_int',
# 	      'rsq' => '!pos_int',
# 	      'img' => '!extid(php)',
# 	      'dcr' => '2012-07-09 04:49:25',
# 	      'dmd' => '!date' };

# my $ob_t1 = { 'parent_no' => '71894',
# 	      'parent_name' => 'Dascillus',
# 	      'phylum' => 'Arthropoda',
# 	      'phylum_no' => '18891',
# 	      'class' => 'Insecta',
# 	      'class_no' => '56637',
# 	      'order' => 'Coleoptera',
# 	      'order_no' => '69148',
# 	      'family' => 'Dascillidae',
# 	      'family_no' => '69296',
# 	      'genus' => 'Dascillus',
# 	      'genus_no' => '71894',
# 	      'lft' => '!pos_int',
# 	      'rgt' => '!pos_int',
# 	      'image_no' => '!pos_int',
# 	      'created' => '2012-07-09 04:49:25',
# 	      'modified' => '!date' };



# my $ob_j2 = { 'noc' => '!>=:4000',
# 	      'odc' => '!>=:80',
# 	      'fmc' => '!>=:200',
# 	      'gnc' => '!>=:900',
# 	      'spc' => '!>=:1200',
# 	      'jmo' => qr{mobile}i,
# 	      'jmc' => '!nonempty',
# 	      'jlh' => '!nonempty',
# 	      'jhc' => '!nonempty',
# 	      'jdt' => '!nonempty',
# 	      'jdc' => '!nonempty',
# 	      'jco' => 'hydroxyapatite',
# 	      'jsa' => qr{compact},
# 	      'jtc' => '!nonempty',
# 	      'ati' => '!extid(prs)',
# 	      'eni' => '!extid(prs)',
# 	      'mdi' => '!extid(prs)',
# 	      'ath' => '!nonempty',
# 	      'ent' => '!nonempty',
# 	      'mdf' => '!nonempty' };

# my $ob_t2 = { 'n_occs' => '!>=:4000',
# 	      'n_orders' => '!>=:80',
# 	      'n_families' => '!>=:200',
# 	      'n_genera' => '!>=:900',
# 	      'n_species' => '!>=:1200',
# 	      'motility' => qr{mobile}i,
# 	      'motility_basis' => '!nonempty',
# 	      'life_habit' => '!nonempty',
# 	      'life_habit_basis' => '!nonempty',
# 	      'diet' => '!nonempty',
# 	      'diet_basis' => '!nonempty',
# 	      'composition' => 'hydroxyapatite',
# 	      'taphonomy_basis' => '!nonempty',
# 	      'authorizer_no' => '!pos_int',
# 	      'enterer_no' => '!pos_int',
# 	      'modifier_no' => '!pos_int',
# 	      'authorizer' => '!nonempty',
# 	      'enterer' => '!nonempty',
# 	      'modifier' => '!nonempty' };

#     # Make sure we can fetch the appropriate output blocks
    
#     my ($r_j1) = $T->fetch_records("/taxa/single.json?name=$OB_NAME_1&show=$OB_BLOCKS_1", 
# 				  'output blocks 1 json');
    
#     unless ( $r_j1 )
#     {
# 	diag("skipping remainder of subtest");
# 	return;
#     }
    
#     my ($r_t1) = $T->fetch_records("/taxa/single.txt?name=$OB_NAME_1&show=$OB_BLOCKS_1",
# 				  'output blocks 1 txt');
    
#     my ($r_j2) = $T->fetch_records("/taxa/single.json?name=$OB_NAME_2&show=$OB_BLOCKS_2",
# 				   'output blocks 2 json');
    
#     my ($r_t2) = $T->fetch_records("/taxa/single.txt?name=$OB_NAME_2&show=$OB_BLOCKS_2",
#     				  'output blocks 2 txt');
    
#     # Check the data fields in the JSON response.  Skip the tests if we didn't
#     # get any record, because this subtest will already have failed above.
    
#     if ( $r_j1 )
#     {
# 	$T->check_fields($r_j1, $ob_j1, 'output blocks 1 json');
#     }
    
#     # Check the data fields in the TXT response, and add some other checks as
#     # well.  Again, skip the tests if we didn't get any record, because this
#     # subtest will already have failed above.
    
#     if ( $r_t1 )
#     {
# 	$T->check_fields($r_t1, $ob_t1, 'output blocks 1 txt');
	
# 	# Make sure that the value of phylum_no matches up to phylum, and so
# 	# on for the other ranks returned by classext.
	
# 	foreach my $f ( qw(phylum class order family) )
# 	{
# 	    my $taxon_no = $r_t1->{"${f}_no"};
# 	    my $taxon_name = $r_t1->{$f};
	    
# 	    ok( $taxon_no, "classext found '${f}_no'" ) || next;
# 	    ok( $taxon_name, "classext found '$f'" ) || next;
	    
# 	    my ($rr) = $T->fetch_records("/taxa/single.txt?id=$taxon_no");
	    
# 	    is( $rr->{taxon_name}, $taxon_name, "classext '${f}_no' matches '$f'" );
# 	}
	
# 	# Check that the modified date is later than the created date.
	
# 	cmp_ok( $r_t1->{modified}, 'ge', $r_t1->{created}, "crmod 'modified' ge 'created'" );
	
# 	# Check that the rgt value is equal to lft if $OB_NAME_2 is a species,
# 	# or > if it is a higher taxon.
	
# 	if ( $r_t1->{taxon_rank} eq 'species' )
# 	{
# 	    cmp_ok( $r_t1->{rgt}, '==', $r_t1->{lft}, "seq 'rgt' == 'lft'" );
# 	}
# 	else
# 	{
# 	    cmp_ok( $r_t1->{rgt}, '>', $r_t1->{lft}, "seq 'rgt' >= 'lft'" );
# 	}
#     }
    
#     # Now check the JSON fields for the second set of output blocks, same as
#     # above.
    
#     if ( $r_j2 )
#     {
#     	$T->check_fields($r_j2, $ob_j2, 'output blocks 2 json');
#     }
    
#     # Check the TXT fields for the second set of output blocks, and do some
#     # additional checks as well.
    
#     if ( $r_t2 )
#     {
# 	$T->check_fields($r_t2, $ob_t2, 'output blocks 2 txt');
	
# 	# Check that the ecospace and taphonomy basis fields actually have the
# 	# attribute values that are attributed to them.
	
# 	foreach my $f ( qw(motility life_habit diet) )
# 	{
# 	    my $basis = $r_t2->{"${f}_basis"};
	    
# 	    ok( $r_t2->{$f}, "ecospace found '$f'" ) || next;
# 	    ok( $basis, "ecospace found '${f}_basis'" ) || next;
	    
# 	    my ($rr) = $T->fetch_records("/taxa/single.txt?name=$basis&show=ecospace");
	    
# 	    is( $rr->{$f}, $r_t2->{$f}, "classext '$f' matches '${f}_basis'" );
# 	}
	
# 	my $basis = $r_t2->{taphonomy_basis};
	
# 	if ( ok( $basis, "taphonomy found 'taphonomy_basis'" ) )
# 	{
# 	    my ($rr) = $T->fetch_records("/taxa/single.txt?name=$basis&show=taphonomy");
	    
# 	    is( $rr->{composition}, $r_t2->{composition},
# 		"taphonomy 'composition' matches 'taphonomy_basis'" );
# 	    is( $rr->{architecture}, $r_t2->{architecture},
# 		"taphonomy 'architecture' matches 'taphonomy_basis'" );
# 	}
#     }
# };


