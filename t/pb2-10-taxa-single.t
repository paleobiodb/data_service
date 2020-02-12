# -*- mode: CPerl -*-
# 
# PBDB 1.2
# --------
# 
# The purpose of this file is to test the /data1.2/taxa/single operation,
# including all of the output blocks and special parameters.
# 

use strict;
use feature 'unicode_strings';

use Test::Most tests => 11;

use lib 't';
use Tester;
use Test::Conditions;
use Test::Selection;


choose_subtests(@ARGV);

# Start by creating an instance of the Tester class, with which to conduct the
# following tests.

my $T = Tester->new({ prefix => 'data1.2' });


# First define the values we will be using to check the taxonomy operations.
# These are representative taxa from the database.

my $TEST_NAME_1 = 'Dascillidae';
my $TEST_NAME_2 = 'Dascilloidea';

my $TEST_NO_1 = '69296';
my $TEST_TXN_1 = 'txn:69296';

my $VT_ID_1 = '71894';
my $VT_ID_1v = '285777';
my $VT_TXN_1 = 'txn:71894';
my $VT_VAR_1 = 'var:285777';
my $VT_VAR_1a = 'var:71894';
my $VT_TXNNAME_1 = 'Dascillus';
my $VT_VARNAME_1 = 'Dascyllus';

my $TEST_REF_NAME_1 = 'Felinae';	# Note, it is important that the
                                        # reference for this contains accented
                                        # characters, and with markrefs has
                                        # both <i> and <b>.

my $TEST_NAME_3 = 'Acila (Truncacila) princeps';

my $TEST_IMAGE_1 = 910;
my $TEST_IMAGE_SIZE_1a = 2047;
my $TEST_IMAGE_SIZE_1b = 1302;

my $TEST_LIST_1 = "felis,canis";
my $TEST_LIST_2 = "txn:41055,txn:41198";

my $t1 = { 'nam' => $TEST_NAME_1,
	   'oid' => $TEST_TXN_1,
	   'rnk' => 9,
	   'nm2' => "soft bodied plant beetle",
	   'att' => "Guerin-Meneville 1843",
	   'rid' => '!extid(ref)',
	   'tdf' => '!empty',
	   'acc' => '!empty',
	   'acn' => '!empty',
	   "phl" => "Arthropoda",
	   "cll" => "Insecta",
	   "odl" => "Coleoptera",
	   "fml" => "Dascillidae",
	   "oid" => '!extid(txn)',
	   "ext" => '!pos_int',
	   "fea" => '!nonzero',
	   "fla" => '!nonzero',
	   "lea" => '!nonzero',
	   "lla" => '!numeric',
	   "siz" => '!pos_int',
	   "exs" => '!pos_int'};
	   
my $t1t = { 'taxon_name' => $TEST_NAME_1,
	    'record_type' => "txn",
	    'taxon_rank' => 'family',
	    'common_name' => "soft bodied plant beetle",
	    'taxon_attr' => "Guerin-Meneville 1843",
	    'reference_no' => '!pos_int',
	    'difference' => '',
	    'accepted_no' => $TEST_NO_1,
	    'accepted_rank' => 'family',
	    'accepted_name' => $TEST_NAME_1,
	    "phylum" => "Arthropoda",
	    "class" => "Insecta",
	    "order" => "Coleoptera",
	    "family" => "Dascillidae",
	    'taxon_no' => '!pos_int',
	    'orig_no' => $TEST_NO_1,
	    'parent_no' => '!pos_int',
	    'is_extant' => '!nonempty',
	    "firstapp_max_ma" => '!nonzero',
	    "firstapp_min_ma" => '!nonzero',
	    "lastapp_max_ma" => '!nonzero',
	    "lastapp_min_ma" => '!numeric',
	    "taxon_size" =>'!pos_int',
	    "extant_size" => '!pos_int' };




my $NAME_WC_1 = "T. rex";
my $PAT_WC_1 = qr{^T\w+ rex$};
my $NAME_WC_2 = 'Pant%rinae';
my $PAT_WC_2 = qr{^Pant.*rinae$};
my $NAME_WC_3 = 'Panth_rinae';
my $PAT_WC_3 = qr{^Panth.rinae$};
my $NAME_WC_4 = 'Pant.rinae';
my $NAME_WC_5 = 'Tyrannos%rex';



# Then do some initial fetches using the taxon name.  Once we get a taxon
# identifier back, we will use that to test fetching by identifier.
# 
# We check the two parameter aliases 'name' and 'taxon_name', and we also
# check both .json and .txt responses.

my ($taxon_id, $parent_id);

subtest 'single json by name' => sub {

    select_subtest || return;
    
    my ($r, $s) = $T->fetch_records("/taxa/single.json?name=$TEST_NAME_1&show=attr,app,size,class,common",
				    "single json by name request OK");
    
    unless ( $r )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    $taxon_id = $r->{oid};
    $parent_id = $r->{par};
    
    unless ( $taxon_id && $parent_id )
    {
	diag("skipping remainder of test");
	exit;
    }
    
    # Check the data fields
    
    $T->check_fields($r, $t1, "single json by name");
    
    # Check that paramter 'taxon_name' also fetches the proper taxon.
    
    my ($r2) = $T->fetch_records("/taxa/single.json?taxon_name=$TEST_NAME_1", "single json taxon_name");
    
    is( $r2->{nam}, $TEST_NAME_1, 'single json found proper taxon_name');
    
    # Check that a name with a subgenus works okay.
    
    my ($r3) = $T->fetch_records("/taxa/single.json?taxon_name=$TEST_NAME_3",
				 "single by name with subgenus");
    
    is( $r3->{nam}, $TEST_NAME_3, "single by name with subgenus fetched proper record" );
    
    # Check that we get an error if more than one name is given.
    
    my $m4 = $T->fetch_nocheck("/taxa/single.json?name=$TEST_LIST_1", "single with multiple names");
    
    $T->ok_response_code( $m4, '400', "single with multiple names got '400' response" );
};


subtest 'single txt by name' => sub {

    select_subtest || return;
    
    my ($r) = $T->fetch_records("/taxa/single.txt?name=$TEST_NAME_1&show=attr,app,size,class,common",
				"single txt by name");
    
    unless ( $r )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    # Check the data fields
    
    $T->check_fields($r, $t1t, "single txt");
};


# Now we fetch the same taxon using the identifier retrieved above.  We check
# the two parameter aliases 'id' and 'taxon_id'.  We don't do a separate check
# for .txt; because the implementation of Web::DataService separates parameter
# processing from expression of data in a particular format, we assume that if
# .txt works fine in the above test then it will work fine with any set of
# parameters.

subtest 'single json by id' => sub {
    
    select_subtest || return;
    
    my ($r) = $T->fetch_records("/taxa/single.json?id=$taxon_id", "single json by id");
    
    unless ( $r )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    is( $r->{nam}, $TEST_NAME_1, "single json by id retrieves proper record" );
    
    # Also check that paramter 'taxon_id' retrieves the same record.
    
    my ($r2) = $T->fetch_records("/taxa/single.json?taxon_id=$taxon_id", 'single json taxon_id');
    
    is( $r2->{nam}, $TEST_NAME_1, "single json taxon_id retrieves proper record" );
    
    my $m2 = $T->fetch_nocheck("/taxa/single.json?id=$TEST_LIST_2", "single with multiple ids");
    
    $T->ok_response_code( $m2, '400', "single with multiple ids got '400' response" );
};


# Test the use of identifiers of the form 'txn:nnnn' and 'var:nnnn' and also
# the 'exact' parameter with both name and numeric id.  The accepted variant
# of the taxon should be returned in all circumstances *unless* an identifier
# with the prefix 'var' is presented or the 'exact' parameter is included.

subtest 'single json name variants' => sub {
    
    select_subtest || return;
    
    my ($r_txn) = $T->fetch_records("/taxa/single.json?id=$VT_TXN_1", 
				    'single variant txn');

    unless ( $r_txn )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    my ($r_txn_ex) = $T->fetch_records("/taxa/single.json?id=$VT_TXN_1&exact", 
				       'single variant txn exact');
    my ($r_var) = $T->fetch_records("/taxa/single.json?id=$VT_VAR_1", 
				    'single variant var');
    my ($r_var_ex) = $T->fetch_records("/taxa/single.json?id=$VT_VAR_1&exact", 
				       'single variant var exact');
    my ($r_id) = $T->fetch_records("/taxa/single.json?id=$VT_ID_1", 
				   'single variant id');
    my ($r_id_ex) = $T->fetch_records("/taxa/single.json?id=$VT_ID_1&exact", 
				      'single variant id exact');
    my ($r_idv) = $T->fetch_records("/taxa/single.json?id=$VT_ID_1v", 
				   'single variant idv');
    my ($r_idv_ex) = $T->fetch_records("/taxa/single.json?id=$VT_ID_1v&exact", 
				      'single variant idv exact');
    my ($r_txnname) = $T->fetch_records("/taxa/single.json?name=$VT_TXNNAME_1", 
					'single variant txnname');
    my ($r_txnname_ex) = $T->fetch_records("/taxa/single.json?name=$VT_TXNNAME_1&exact", 
					   'single variant txnname exact');
    my ($r_varname) = $T->fetch_records("/taxa/single.json?name=$VT_VARNAME_1",
					'single variant varname');
    my ($r_varname_ex) = $T->fetch_records("/taxa/single.json?name=$VT_VARNAME_1&exact",
					   'single variant varname exact');
    
    if ( $r_txn )
    {
	is($r_txn->{nam}, $VT_TXNNAME_1, 'single variant txn has proper name');
	is($r_txn->{oid}, $VT_TXN_1, 'single variant txn has proper oid');
	is($r_txn->{vid}, undef, 'single variant txn has proper vid');
    }
    
    if ( $r_txn_ex )
    {
	is($r_txn_ex->{nam}, $VT_TXNNAME_1, 'single variant txn exact has proper name');
	is($r_txn_ex->{oid}, $VT_TXN_1, 'single variant txn exact has proper oid');
	is($r_txn_ex->{vid}, undef, 'single variant txn exact has proper vid');
    }
    
    if ( $r_var )
    {
	is($r_var->{nam}, $VT_VARNAME_1, 'single variant var has proper name');
	is($r_var->{oid}, $VT_TXN_1, 'single variant var has proper oid');
	is($r_var->{vid}, $VT_VAR_1, 'single variant var has proper vid');
    }
    
    if ( $r_var_ex )
    {
	is($r_var_ex->{nam}, $VT_VARNAME_1, 'single variant var exact has proper name');
	is($r_var_ex->{oid}, $VT_TXN_1, 'single variant var exact has proper oid');
	is($r_var_ex->{vid}, $VT_VAR_1, 'single variant var exact has proper vid');
    }
    
    if ( $r_id )
    {
	is($r_id->{nam}, $VT_TXNNAME_1, 'single variant id has proper name');
	is($r_id->{oid}, $VT_TXN_1, 'single variant id has proper oid');
	is($r_id->{vid}, undef, 'single variant id has proper vid');
    }
    
    if ( $r_id_ex )
    {
	is($r_id_ex->{nam}, $VT_TXNNAME_1, 'single variant id exact has proper name');
	is($r_id_ex->{oid}, $VT_TXN_1, 'single variant id exact has proper oid');
	is($r_id_ex->{vid}, undef, 'single variant id exact has proper vid');
    }
    
    if ( $r_idv )
    {
	is($r_idv->{nam}, $VT_TXNNAME_1, 'single variant idv has proper name');
	is($r_idv->{oid}, $VT_TXN_1, 'single variant idv has proper oid');
	is($r_idv->{vid}, undef, 'single variant idv has proper vid');
    }
    
    if ( $r_idv_ex )
    {
	is($r_idv_ex->{nam}, $VT_VARNAME_1, 'single variant idv exact has proper name');
	is($r_idv_ex->{oid}, $VT_TXN_1, 'single variant idv exact has proper oid');
	is($r_idv_ex->{vid}, $VT_VAR_1, 'single variant idv exact has proper vid');
    }
    
    if ( $r_txnname )
    {
	is($r_txnname->{nam}, $VT_TXNNAME_1, 'single variant txnname has proper name');
	is($r_txnname->{oid}, $VT_TXN_1, 'single variant txnname has proper oid');
	is($r_txnname->{vid}, undef, 'single variant txnname has proper vid');
    }
    
    if ( $r_txnname_ex )
    {
	is($r_txnname_ex->{nam}, $VT_TXNNAME_1, 'single variant txnname exact has proper name');
	is($r_txnname_ex->{oid}, $VT_TXN_1, 'single variant txnname exact has proper oid');
	is($r_txnname_ex->{vid}, undef, 'single variant txnname exact has proper vid');
    }
    
    if ( $r_varname )
    {
	is($r_varname->{nam}, $VT_VARNAME_1, 'single variant varname has proper name');
	is($r_varname->{oid}, $VT_TXN_1, 'single variant varname has proper oid');
	is($r_varname->{vid}, $VT_VAR_1, 'single variant varname has proper vid');
    }
    
    if ( $r_varname_ex )
    {
	is($r_varname_ex->{nam}, $VT_VARNAME_1, 'single variant varname exact has proper name');
	is($r_varname_ex->{oid}, $VT_TXN_1, 'single variant varname exact has proper oid');
	is($r_varname_ex->{vid}, $VT_VAR_1, 'single variant varname exact has proper vid');
    }
    
};


# Test that the parent id returned from a subtest above can itself be fetched
# from the database.

subtest 'parent json' => sub {
    
    select_subtest || return;
    
    my ($r) = $T->fetch_records("/taxa/single.json?id=$parent_id", 'parent json');
    
    unless ( $r )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    my $taxon_name = $r->{nam};
    ok( defined $taxon_name, 'parent json taxon name' ) or return;
    is( $taxon_name, $TEST_NAME_2, "parent json retrieves proper record" );
};


# Check that wildcards work in names.

subtest 'single by name with wildcards' => sub {
    
    select_subtest || return;
    
    # Check names with wildcards to make sure they give the expected response.
    
    my ($w1) = $T->fetch_records("/taxa/single.json?name=$NAME_WC_1", "single with wildcard '.'");
    
    unless ( $w1 )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    my ($w2) = $T->fetch_records("/taxa/single.json?name=$NAME_WC_2", "single with wildcard '%'");
    my ($w3) = $T->fetch_records("/taxa/single.json?name=$NAME_WC_3", "single with wildcard '_'");
    my ($m4) = $T->fetch_nocheck("/taxa/single.json?name=$NAME_WC_4", "single with inline '.'");
    my ($m5) = $T->fetch_nocheck("/taxa/single.json?name=$NAME_WC_5", "single with inline '%'");
    
    if ( $w1 )
    {
	like( $w1->{nam}, $PAT_WC_1, "single with wildcard '.' found proper record" );
    }
    
    if ( $w2 )
    {
	like( $w2->{nam}, $PAT_WC_2, "single with wildcard '%' found proper record" );
    }
    
    if ( $w3 )
    {
	like( $w3->{nam}, $PAT_WC_3, "single with wildcard '_' found proper record" );
    }
    
    $T->ok_response_code( $m4, '404', "single with inline '.' got 404" );
    $T->ok_response_code( $m5, '404', "single with inline '%' got 404" );
    
    # Check to make sure that the one with the most occurrences is being
    # returned. 
    
    my (@w1) = $T->fetch_records("/taxa/list.json?match_name=$NAME_WC_1", "match with wildcard '.'");
    
    my ($max_occs, $max_id);
    
    foreach my $r (@w1)
    {
	if ( !defined $max_occs || $r->{noc} > $max_occs )
	{
	    $max_occs = $r->{noc};
	    $max_id = $r->{oid};
	}
    }
    
    is( $max_id, $w1->{oid}, "single with wildcard '.' found max occs" );
};


# Now test the 'nav' block of single taxon output.

subtest 'nav block' => sub {
    
    select_subtest || return;
    
    my ($r1) = $T->fetch_records("/taxa/single.json?name=$TEST_NAME_1&show=nav,parent", "single with nav block");
    my ($r1c) = $T->fetch_records("/taxa/single.json?name=$TEST_NAME_1&show=classext", "single with classext block");
    
    my ($ipr) = $T->fetch_records("/taxa/list.json?name=$TEST_NAME_1&rel=immpar", "single immpar");
    my ($spr) = $T->fetch_records("/taxa/list.json?name=$TEST_NAME_1&rel=senpar", "single senpar");
    
    cmp_ok( $ipr->{nam}, 'eq', $r1->{ipl}, "immpar label is correct" );
    cmp_ok( $spr->{nam}, 'eq', $r1->{prl}, "senpar label is correct" );
    cmp_ok( $spr->{rnk}, 'eq', $r1->{prr}, "senpar rank is correct" );
    
    # First check that we have a 'prt' subrecord and that its basic values are okay.
    
    if ( ok( ref $r1->{prt} eq 'HASH', "single with nav bock has 'prt'" ) )
    {
	cmp_ok( $r1->{prt}{oid}, 'eq', $spr->{oid}, "prt oid is correct" );
	if ( $spr->{vid} )
	{
	    cmp_ok( $r1->{prt}{vid}, 'eq', $spr->{vid}, "prt vid is correct" );
	}
	cmp_ok( $r1->{prt}{nam}, 'eq', $spr->{nam}, "prt label is correct" );
	cmp_ok( $r1->{prt}{rnk}, 'eq', $spr->{rnk}, "prt rank is correct" );
	cmp_ok( $r1->{prt}{siz}, '>', 0, "prt size is correct" );
	cmp_ok( $r1->{prt}{exs}, '>', 0, "prt extant_size is correct" );
    }
    
    # Next check that we have a 'kgt' subrecord and that its basic values are okay.
    
    if ( ok( ref $r1->{kgt} eq 'HASH', "single with nav block has 'kgt'" ) )
    {
	cmp_ok( $r1->{kgl}, 'eq', $r1->{kgt}{nam}, "kgt label is correct" );
	cmp_ok( $r1->{kgn}, 'eq', $r1->{kgt}{oid}, "kgt oid is correct" );
	cmp_ok( $r1->{kgt}{rnk}, '>', 21, "kgt rank is correct" );
	cmp_ok( $r1->{kgt}{siz}, '>', 0, "prt size is greater than zero" );
	cmp_ok( $r1->{kgt}{exs}, '>', 0, "prt extant_size is greater than zero" );
    }
    
    # Now check the subtaxon records for phylum, class, order, family
    
    foreach my $prefix ( qw(ph cl od fm) )
    {
	next unless ok( ref $r1->{"${prefix}t"} eq 'HASH', "single with nav block has '${prefix}t'" );
	
	cmp_ok( $r1->{"${prefix}t"}{nam}, 'eq', $r1c->{"${prefix}l"}, "${prefix}t name is correct" );
	cmp_ok( $r1->{"${prefix}t"}{oid}, 'eq', $r1c->{"${prefix}n"}, "${prefix}t oid is correct" );
	cmp_ok( $r1->{"${prefix}t"}{rnk}, '>', 5, "${prefix}t rank is greater than minimum" );
	cmp_ok( $r1->{"${prefix}t"}{siz}, '>', 0, "${prefix}t size is greater than zero" );
	cmp_ok( $r1->{"${prefix}t"}{exs}, '>', 0, "${prefix}t extant_size is greater than zero" );
    }
    
    # Now check the subtaxon records for children and genera
    
    my $tc = Test::Conditions->new;
    $tc->limit_max( fea => 3 );
    
    foreach my $key ( qw(chl gns) )
    {
	next unless ok( ref $r1->{$key} eq 'ARRAY', "single with nav block has '$key' array" );
	
	foreach my $t ( @{$r1->{$key}} )
	{
	    my $oid = $t->{oid};
	    
	    $tc->flag('oid', 'MISSING VALUE') unless $oid && $oid =~ /^txn:\d+$/;
	    $tc->flag('nam', $oid) unless $t->{nam};
	    $tc->flag('rnk', $oid) unless $t->{rnk} && $t->{rnk} > 0;
	    $tc->flag('siz', $oid) unless $t->{siz} && $t->{siz} > 0;
	    $tc->flag('exs', $oid) unless defined $t->{exs} && $t->{exs} >= 0;
	    # $tc->flag('fea', $oid) unless $t->{fea} && $t->{fea} > 0;
	}
    }
    
    $tc->ok_all("chl and gns records are correct");
};


subtest 'images' => sub {
    
    select_subtest || return;
    
    my $thumb = $T->fetch_url("/taxa/thumb.png?id=$TEST_IMAGE_1",
			      "image thumb request OK") || return;
    
    my $thumb_length = length($thumb->content) || 0;
    
    cmp_ok($thumb_length, '==', $TEST_IMAGE_SIZE_1a, 'image thumb size');
    
    my $icon = $T->fetch_url("/taxa/icon.png?id=910",
			     "image icon request OK") || return;
    
    my $icon_length = length($icon->content) || 0;
    
    cmp_ok($icon_length, '==', $TEST_IMAGE_SIZE_1b, 'image icon size');
};


# Some of the special parameters have already been tested above: 'limit',
# 'offset', 'rowcount', 'datainfo'.

# Now we test the others: 'strict', 'textresult', 'save', 'markrefs',
# 'noheader', 'lb', 'header'.

subtest 'special params' => sub {
    
    select_subtest || return;
    
    # First test 'strict'. Compare the results of two queries, one with
    # strict and one without.
    
    my $strict = $T->fetch_nocheck("/taxa/single.json?name=badtaxon&strict", "special 'strict'");
    my $loose = $T->fetch_nocheck("/taxa/single.json?name=badtaxon&strict=no", "special 'strict=no'");
    
    unless ( $strict && $T->get_response_code($strict) ne '500' )
    {
	fail("bad response from server");
	diag("skipping remainder of subtest");
	return;
    }
    
    $T->cmp_ok_errors($strict, '==', 1, "special 'strict' returns one error" );
    $T->cmp_ok_warnings($strict, '==', 1, "special 'strict' returns one warning" );
    $T->ok_warning_like($strict, qr{badtaxon}, "special 'strict' proper warning" );
    
    $T->cmp_ok_errors($loose, '==', 1, "special 'strict=no' returns one error" );
    $T->cmp_ok_warnings($loose, '<', 2, "special 'strict=no' returns at most one warning" );
    $T->ok_warning_like($loose, qr{badtaxon}, "special 'strict=no' proper warning" );
    
    # Now we check 'textresult'. This one is easy, we just need to check that the
    # content type has the correct value and the content does nto change.
    
    my $textresult = $T->fetch_url( "/taxa/single.csv?name=$TEST_NAME_1&textresult", 
				    "special 'textresult'" );
    my $csvresult = $T->fetch_url( "/taxa/single.csv?name=$TEST_NAME_1", 
				   "special no 'textresult'" );
    
    $T->ok_content_type($textresult, 'text/plain', 'utf-?8', "special 'textresult' content type" );
    $T->ok_content_type($csvresult, 'text/csv', 'utf-?8', "special 'textresult=no' content type" );
    
    is( $textresult->content, $csvresult->content, "special 'textresult' same content" );
    
    # Now we check 'save', both with and without a parameter value. We also
    # check that .txt format does not generate a disposition header while .csv
    # and .tsv do.
    
    my $h1 = 'Content-Disposition';
    
    my $tsvresult = $T->fetch_url( "/taxa/single.tsv?name=$TEST_NAME_1", "tsv result" );
    my $txtresult = $T->fetch_url( "/taxa/single.txt?name=$TEST_NAME_1", "txt result" );
    
    my $saveresult1 = $T->fetch_url( "/taxa/single.csv?name=$TEST_NAME_1&save", 
				     "special 'save'" );
    my $saveresult2 = $T->fetch_url( "/taxa/single.csv?name=$TEST_NAME_1&save=foo", 
				     "special 'save=foo'" );
    my $saveresult3 = $T->fetch_url( "/taxa/single.csv?name=$TEST_NAME_1&save=foo.bar", 
				     "special 'save=foo.bar'" );
    
    is( $csvresult->header($h1), 'attachment; filename="pbdb_data.csv"',
	"csv result with no 'save' has proper disposition header" );
    is( $tsvresult->header($h1), 'attachment; filename="pbdb_data.tsv"',
	"tsv result with no 'save' has proper disposition header" );
    ok( !defined $txtresult->header($h1), "txt result with no 'save' has no disposition header" );
    
    is( $saveresult1->header($h1), 'attachment; filename="pbdb_data.csv"',
	"special 'save' returns proper disposition header" );
    is( $saveresult2->header($h1), 'attachment; filename="foo.csv"',
	"special 'save=foo' returns proper disposition header" );
    is( $saveresult3->header($h1), 'attachment; filename="foo.bar"',
	"special 'save=foo.bar' returns proper disposition header" );
    
    # Now test 'markrefs'. We fetch the same result with and without, and make
    # sure that the marks are the only difference.
    
    my ($r_marked) = $T->fetch_records( "/taxa/single.txt?name=$TEST_REF_NAME_1&show=ref&markrefs",
					"special 'markrefs'" );
    my ($r_unmarked) = $T->fetch_records( "/taxa/single.txt?name=$TEST_REF_NAME_1&show=ref&markrefs=no",
					  "special 'markrefs=no'" );
    my ($r_default) = $T->fetch_records( "/taxa/single.txt?name=$TEST_REF_NAME_1&show=ref",
					 "special no 'markrefs'" );
    
    my $ref_check = $r_marked->{primary_reference};
    $ref_check =~ s{ < /? [ib] > }{}xg;
    
    my $ref_marked = $r_marked->{primary_reference};
    my $ref_unmarked = $r_unmarked->{primary_reference};
    my $ref_default = $r_default->{primary_reference};
    
    like( $ref_marked, qr{<i>}, "marked ref txt has <i>" );
    like( $ref_marked, qr{<b>}, "marked ref txt has <b>" );
    is( $ref_check, $ref_unmarked, "marked ref txt differs only in <b> and <i>" );
    is( $ref_unmarked, $ref_default, "special 'markrefs' default is 'no'" );
    
    my ($rj_marked) = $T->fetch_records( "/taxa/single.json?name=$TEST_REF_NAME_1&show=ref&markrefs",
					 "special 'markrefs' json");
    my ($rj_unmarked) = $T->fetch_records( "/taxa/single.json?name=$TEST_REF_NAME_1&show=ref",
					   "special no 'markrefs' json" );
    
    my $refj_check = $rj_marked->{ref};
    $refj_check =~ s{ < /? [ib] > }{}xg;
    
    my $refj_marked = $rj_marked->{ref};
    my $refj_unmarked = $rj_unmarked->{ref};
    
    like( $refj_marked, qr{<i>}, "marked ref json has <i>" );
    like( $refj_marked, qr{<b>}, "marked ref json has <b>" );
    is( $refj_check, $refj_unmarked, "marked ref json differs only in <b> and <i>" );
    
    # As a bonus, we also check that accented characters in references are
    # reported properly. So the record selected for this test should have
    # accented characters in it.
    
    use utf8;
    
    like( $ref_marked, qr{ó}, "ref has character 'ó'" );
    like( $ref_marked, qr{ñ}, "ref has character 'ñ'" );
    
    like( $refj_marked, qr{ó}, "ref has character 'ó'" );
    like( $refj_marked, qr{ñ}, "ref has character 'ñ'" );
    
    no utf8;
    
    # Now we test the 'lb' parameter.
    
    my $cr_result = $T->fetch_url( "/taxa/single.txt?name=$TEST_REF_NAME_1&lb=cr" );
    
    my $csv_content = $csvresult->content;
    my $cr_content = $cr_result->content;
    
    ok( $csv_content =~ /\r\n/, "csv result contains crlf" );
    ok( $cr_content =~ /\r"/, "special 'lb=cr' returns result with cr" );
    
    # Finally, we test the 'header' and 'noheader' parameters.
    
    my $nohead_result = $T->fetch_url( "/taxa/single.csv?name=$TEST_REF_NAME_1&noheader" );
    my $headno_result = $T->fetch_url( "/taxa/single.csv?name=$TEST_REF_NAME_1&header=no" );
    my $headyes_result = $T->fetch_url( "/taxa/single.csv?name=$TEST_REF_NAME_1&header=yes" );
    
    my $nohead_content = $nohead_result->content;
    my $headno_content = $headno_result->content;
    my $headyes_content = $headyes_result->content;
    
    ok( $csv_content =~ /"orig_no"/, "csv result contains header" );
    ok( $nohead_content !~ /"orig_no"/, "noheader result does not contain header" );
    ok( $headno_content !~ /"orig_no"/, "header=no result does not contain header" );
    ok( $headyes_content =~ /"orig_no"/, "header=yes result contains header" );
};


# Then we test some bad parameters to make sure we get the proper responses

subtest 'bad params' => sub {
    
    select_subtest || return;
    
    # Then try an unrecognized parameter
    
    my $m2 = $T->fetch_nocheck( "/taxa/single.json?id=abc_def&foo=bar", "bad param 'foo'" );
    
    unless ( $m2 )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    $T->ok_response_code( $m2, '400', "bad param 'foo' got 400 response" );
    $T->ok_error_like($m2, qr{parameter 'foo'}i, "bad param 'foo' got proper error" );
    
    # Then try an invalid identifier
    
    my $m1 = $T->fetch_nocheck( "/taxa/single.json?id=abc_def", "bad param 'id'" );
    
    $T->ok_response_code( $m1, '400', "bad param 'id' got 400 response" );
    $T->ok_error_like( $m1, qr{valid identifier}i, "bad param 'id' got proper error" );
    
    # Then try multiple valid identifiers
    
    my $m3 = $T->fetch_nocheck( "/taxa/single.json?id=txn:234,txn:345", "multiple ids" );
    
    $T->ok_response_code( $m3, '400', "multiple ids got 400 response" );
    $T->ok_error_like( $m3, qr{single}i, "multiple ids got proper error" );
    
    # Then try multiple valid names
    
    my $BAD_4 = 'Canis, felis';
    
    my $m4 = $T->fetch_nocheck( "/taxa/single.json?name=$BAD_4", "multiple names" );
    
    $T->ok_response_code( $m4, '400', "multiple names got 400 response" );
    $T->ok_error_like( $m4, qr{single taxon name}i, "multiple names got proper error" );
    
    # Then try multiple names one of which is invalid. We accept either of the
    # specified error messages.
    
    my $BAD_5 = 'Canis, >felis';
    
    my $m5 = $T->fetch_nocheck( "/taxa/single.json?name=$BAD_5", "multiple invalid" );
    
    $T->ok_response_code( $m5, '400', "multiple invalid got 400 response" );
    unless ( $T->ok_error_like( $m5, qr{single taxon name|invalid character}i,
				"multiple invalid got proper error" ) )
    {
	diag("  expected: " . qr{single taxon name}i . ' or ' . qr{invalid character}i);
	$T->diag_errors($m5);
    }
    
    # Now try a single name with an invalid character.
    
    my $BAD_6 = 'Felis @catus';
    
    my $m6 = $T->fetch_nocheck( "/taxa/single.json?name=$BAD_6", "invalid character");
    
    $T->ok_response_code( $m6, '400,404', "invalid character got 400 or 404 response" );
    unless ( ok( $T->has_error_like( $m6, qr{invalid character}i ) ||
		 $T->has_warning_like( $m6, qr{invalid character}i ), 
		 "invalid character got error or warning" ) )
    {
	diag("  expected: " . qr{invalid character}i);
	$T->diag_errors($m6);
	$T->diag_warnings($m6);
    }
    
    # Then try some differently formed names with all valid characters
    
    my $BAD_7a = '(not) a taxon';
    my $GOOD_7b = 'not (a) taxon';
    my $BAD_7c = 'not a (taxon)';
    my $GOOD_7d = 'not a taxon';
    
    my $m7a = $T->fetch_nocheck( "/taxa/single.json?name=$BAD_7a", "name a");
    my $m7b = $T->fetch_nocheck( "/taxa/single.json?name=$GOOD_7b", "name b");
    my $m7c = $T->fetch_nocheck( "/taxa/single.json?name=$BAD_7c", "name c");
    my $m7d = $T->fetch_nocheck( "/taxa/single.json?name=$GOOD_7d", "name d");
    
    $T->ok_response_code( $m7a, '400,404', "name a got 400 or 404 response" );
    $T->ok_response_code( $m7b, '404', "name b got 404 response" );
    $T->ok_response_code( $m7c, '400,404', "name c got 400 or 404 response" );
    $T->ok_response_code( $m7d, '404', "name d got 404 response" );
    
    $T->ok_error_like( $m7a, qr{invalid taxon name}i, "name a got proper error" );
    $T->ok_warning_like( $m7a, qr{pattern}i, "name a got proper warning" );
};


# Now test taxon identifiers indicating "unknown taxon".

subtest 'unknown taxon' => sub {
    
    select_subtest || return;
    
    my $ID1 = 'txn:UF1';
    my $ID2 = 'txn:UF';
    my $ID3 = 'UF1';
    my $ID4 = 'UF';
    
    my ($r1) = $T->fetch_records("/taxa/single.json?id=$ID1", "id1");
    
    unless ( $r1 )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    my ($r2) = $T->fetch_records("/taxa/single.json?id=$ID2", "id2");
    my ($r3) = $T->fetch_records("/taxa/single.json?id=$ID3", "id3");
    my ($r4) = $T->fetch_records("/taxa/single.json?id=$ID4", "id4");
    
    my $tc = Test::Conditions->new;

    foreach my $r ($r1, $r2, $r3, $r4)
    {
	$tc->flag('oid', $r->{oid}) unless $r->{oid} eq $ID1 || $r->{oid} eq $ID2;
	$tc->flag('rnk', $r->{rnk}) unless $r->{rnk} eq '9';
	$tc->flag('nam', $r->{nam}) unless $r->{nam} eq 'UNKNOWN FAMILY';
    }
    
    $tc->ok_all('unknown taxon records');
};
