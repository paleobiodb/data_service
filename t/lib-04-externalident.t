#
# Paleobiology Database library modules
# -------------------------------------
#
# This file contains unit tests for library modules that are used by the EditTransaction
# client modules and by the data service.
#
# lib-04-externalident.t : Test ExternalIdent.pm, which recognizes and generates the
#                          external identifiers of database records, such as 'txn:5' or
#                          'paleobiodb.org:occ:12', and the class PBDB::ExtIdent that
#                          holds a parsed identifier. The EditTransaction module
#                          PaleoBioDB uses it for every record identifier.
#
# These tests do not use the database.
#
# Added by Claude under the guidance of Michael McClennen.
#

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 10;

use ExternalIdent qw(VALID_IDENTIFIER extract_identifier generate_identifier extract_num
		     %IDP %IDRE);


# Validate a value as an identifier of the specified type. Return a description of the
# result: the type and number of the identifier object, the value itself if it is not an
# object, or the error message.

sub check {
    
    my ($type, $value, $context) = @_;
    
    my $result = eval { VALID_IDENTIFIER($type)->($value, $context) };
    
    return "EXCEPTION: " . ($@ =~ s/ at .*//sr) if $@;
    return "ERROR: $result->{error}" if $result->{error};
    return "value $result->{value}" unless ref $result->{value};
    return "$result->{value}{type} $result->{value}{num}";
}


# Every type of identifier has a pattern and a validator, except for the two prefixes
# that may come before the type.

subtest 'types' => sub {
    
    my @types = grep { $_ ne 'URN' && $_ ne 'UMS' } keys %IDP;
    
    ok( @types > 20, "there are many identifier types" );
    
    foreach my $type ( sort @types )
    {
	ok( ref $IDRE{$type} eq 'Regexp' && ref VALID_IDENTIFIER($type) eq 'CODE',
	    "type $type has a pattern and a validator" );
    }
    
    ok( ref VALID_IDENTIFIER('ANY') eq 'CODE', "type ANY has a validator" );
    
    eval { VALID_IDENTIFIER() };
    
    like( $@, qr{requires a parameter}, "VALID_IDENTIFIER requires a type" );
    
    eval { VALID_IDENTIFIER('XYZ') };
    
    like( $@, qr{No validator is defined for 'XYZ'}, "an unknown type is rejected" );
};


# An identifier may be the type followed by the number, or just the number, in any case,
# and may start with one of the standard prefixes.

subtest 'valid identifiers' => sub {
    
    my %ok = ( 'txn:5' => 'txn 5',
	       '5' => 'txn 5',
	       '0' => 'txn 0',
	       'txn:0' => 'txn 0',
	       'TXN:5' => 'txn 5',
	       'paleobiodb.org:txn:5' => 'txn 5',
	       'urn:lsid:paleobiodb.org:txn:5' => 'txn 5',
	       'pbdb:txn:5' => 'txn 5',
	       'txn:123456789' => 'txn 123456789' );
    
    foreach my $value ( sort keys %ok )
    {
	is( check('TXN', $value), $ok{$value}, "TXN '$value'" );
    }
    
    # Each type with a single prefix accepts that prefix.
    
    foreach my $type ( sort grep { $IDP{$_} =~ /^\w+$/ && $_ ne 'CLM' && $_ ne 'UNT' } keys %IDP )
    {
	is( check($type, "$IDP{$type}:12"), "$IDP{$type} 12", "$type '$IDP{$type}:12'" );
    }
    
    # Macrostrat identifiers have their own prefix.
    
    is( check('CLM', 'macrostrat:clm:5'), 'clm 5', "CLM 'macrostrat:clm:5'" );
    is( check('CLM', 'urn:lsid:macrostrat.org:clm:5'), 'clm 5', "CLM with the full Macrostrat prefix" );
    is( check('UNT', 'unit:5'), 'unit 5', "UNT 'unit:5'" );
    like( check('CLM', 'pbdb:clm:5'), qr{^ERROR}, "a Macrostrat identifier with the PBDB prefix" );
    
    # A type with several prefixes accepts any of them.
    
    is( check('TID', 'var:7'), 'var 7', "TID accepts 'var'" );
    is( check('TID', 'txn:7'), 'txn 7', "and 'txn'" );
    like( check('TID', 'occ:7'), qr{^ERROR}, "but not 'occ'" );
    is( check('OID', 'rei:3'), 'rei 3', "OID accepts 'rei'" );
    is( check('PLC', 'gnm:3'), 'gnm 3', "PLC accepts 'gnm'" );
    is( check('ANY', 'occ:5'), 'occ 5', "ANY accepts any PBDB type" );
    like( check('ANY', 'unit:5'), qr{^ERROR}, "but not a Macrostrat type" );
};


# The identifier object holds the type and number. It stringifies to the number, and can
# regenerate the identifier. A taxon identifier also holds the taxon number.

subtest 'identifier objects' => sub {
    
    my $id = VALID_IDENTIFIER('TXN')->('txn:5')->{value};
    
    isa_ok( $id, 'PBDB::ExtIdent' );
    is( "$id", '5', "it stringifies to the number" );
    ok( $id == 5, "and compares as a number" );
    is( $id->type, 'txn', "type" );
    is( $id->regenerate, 'txn:5', "regenerate" );
    is( $id->{taxon_no}, 5, "a taxon identifier has a taxon_no" );
    is( VALID_IDENTIFIER('VAR')->('var:6')->{value}{taxon_no}, 6, "so does a variant" );
    ok( ! exists VALID_IDENTIFIER('OCC')->('occ:7')->{value}{taxon_no}, "an occurrence does not" );
    
    my $unk = PBDB::ExtIdent->new(undef, 9);
    
    is( $unk->type, 'unk', "an object without a type has the type 'unk'" );
    is( $unk->regenerate, '9', "and regenerates as the number" );
    is( PBDB::ExtIdent->new('OCC', 3)->type, 'occ', "the type is in lower case" );
    
    eval { PBDB::ExtIdent->new('txn') };
    
    like( $@, qr{missing arguments}, "an object must have a number" );
    
    eval { PBDB::ExtIdent->new('txn', -1) };
    
    like( $@, qr{missing arguments}, "and the number must not be negative" );
    
    eval { PBDB::ExtIdent->new('txn', 'abc') };
    
    like( $@, qr{missing arguments}, "or a word" );
    
    is( PBDB::ExtIdent->new('txn', 'NA12')->{num}, 'NA12', "an unknown taxon is accepted" );
};


# A taxon that is not in the database has an identifier such as 'txn:NA12'. The forms
# starting with 'U' are changed to 'N'.

subtest 'unknown taxa' => sub {
    
    is( check('TXN', 'txn:NA12'), 'txn NA12', "TXN 'txn:NA12'" );
    is( check('TID', 'NF'), 'txn NF', "TID 'NF'" );
    is( check('TXN', 'UA3'), 'txn NA3', "'UA3' becomes 'NA3'" );
    like( check('OCC', 'NA12'), qr{^ERROR}, "only taxon identifiers may be unknown" );
};


# In a data entry request, a value starting with '@' refers to the label of another
# record, and is returned as it is.

subtest 'labels' => sub {
    
    my $entry = { request => { is_data_entry => 1 } };
    
    is( check('TXN', '@new1', $entry), 'value @new1', "a label in a data entry request" );
    like( check('TXN', '@new1'), qr{^ERROR}, "but not in another request" );
    like( check('TXN', '@new1', { request => { } }), qr{^ERROR}, "or one that is not data entry" );
};


# Values that are not identifiers of the right type get an error message, which the data
# service fills in with the parameter name and value.

subtest 'error messages' => sub {
    
    my %error = (
	'5,6' => "Field {param}: must be a single value",
	'occ:5' => "Field {param}: external identifier must have type txn",
	'foo:5' => "Field {param}: external identifier must have type txn",
	'abc' => "Field {param}: value must be either a valid identifier or a nonnegative integer",
	'-5' => "Field {param}: value must be either a valid identifier or a nonnegative integer",
	'txn:5.5' => "Field {param}: value must be either a valid identifier or a nonnegative integer",
	'' => "Field {param}: value must be either a valid identifier or a nonnegative integer",
	'pbdb:occ:5' => "Field {param}: external identifier must have type txn, optionally " .
	    "prefixed by 'paleobiodb.org:'" );
    
    foreach my $value ( sort keys %error )
    {
	is( check('TXN', $value), "ERROR: $error{$value}", "TXN '$value'" );
    }
    
    is( check('TID', 'abc'), "ERROR: Field {param}: value must be either a valid identifier of " .
	"the form 'txn:nnnn' or 'var:nnnn' where nnnn is an integer, or a nonnegative integer " .
	"(was {value})", "a type with two prefixes names both" );
    is( check('ANY', 'abc'), "ERROR: Field {param}: value must be either a valid identifier of " .
	"the form 'type:nnnn' where nnnn is an integer, or a nonnegative integer (was {value})",
	"ANY" );
  
    is( check('ANY', 'foo:5'), "ERROR: Field {param}: identifier type 'foo' is not accepted",
	"ANY with an unknown type" );
    is( check('ANY', 'pbdb:unit:5'), "ERROR: Field {param}: identifier type 'unit' is not " .
	"accepted, optionally prefixed by 'paleobiodb.org:'", "ANY with a Macrostrat type" );
};


# extract_identifier returns an identifier object if the value is an identifier of the
# specified type, and extract_num returns just the number.

subtest 'extracting identifiers' => sub {
    
    my $id = extract_identifier('OCC', 'occ:42');
    
    isa_ok( $id, 'PBDB::ExtIdent' );
    is( $id->regenerate, 'occ:42', "extract_identifier" );
    is( extract_identifier('OCC', '42')->type, 'unk', "a bare number has the type 'unk'" );
    is( extract_identifier('OCC', 'txn:42'), undef, "an identifier of another type" );
    is( extract_identifier('OCC', 'abc'), undef, "a value that is not an identifier" );
    is( extract_num('OCC', 'paleobiodb.org:occ:42'), 42, "extract_num" );
    is( extract_num('OCC', '42'), 42, "extract_num of a bare number" );
    is( extract_num('OCC', 'txn:42'), undef, "extract_num of another type" );
    is( extract_num('OCC', 'occ:ERROR'), -1, "extract_num of 'ERROR' is -1" );
};


# generate_identifier makes an identifier from a number. 0 gives the empty string, and a
# value that is not a number gives 'type:ERROR'. A list is converted in place.

subtest 'generating identifiers' => sub {
    
    is( generate_identifier('OCC', 42), 'occ:42', "a number" );
    is( generate_identifier('OCC', '42'), 'occ:42', "a number as a string" );
    is( generate_identifier('OCC', 0), '', "0 gives the empty string" );
    is( generate_identifier('OCC', -3), 'occ:ERROR', "a negative number" );
    is( generate_identifier('OCC', 'abc'), 'occ:ERROR', "a value that is not a number" );
    is( generate_identifier('OCC', undef), undef, "undef" );
    is( generate_identifier('TXN', 'NA12'), 'txn:NA12', "an unknown taxon" );
    is( generate_identifier('OCC', 'occ:42'), 'occ:42', "an identifier is returned as it is" );
    
    my @list = (1, 0, 3);
    my @result = generate_identifier('COL', \@list);
    
    is( join(',', @list), 'col:1,,col:3', "a list is converted in place" );
    is( join(',', @result), 'col:1,,col:3', "and the converted values are returned" );
  
    is( generate_identifier('OCC', '12abc'), 'occ:ERROR', "a value that starts with digits" );
    is( generate_identifier('OCC', 1.5), 'occ:ERROR', "a number that is not an integer" );
    
    # The identifiers generated for each single-prefix type can be read back.
    
    foreach my $type ( sort grep { $IDP{$_} =~ /^\w+$/ && $_ ne 'CLM' && $_ ne 'UNT' } keys %IDP )
    {
	is( check($type, generate_identifier($type, 77)), "$IDP{$type} 77",
	    "an identifier generated for $type can be read back" );
    }
};


# 'ERROR' is accepted in place of a number, because generate_identifier produces it for a
# bad value. The validators report it as an error, and extract_identifier returns nothing.

subtest 'identifiers with ERROR' => sub {
    
    ok( 'txn:ERROR' =~ $IDRE{TXN}, "the pattern accepts 'txn:ERROR'" );
  
    is( check('TXN', 'txn:ERROR'), "ERROR: Field {param}: the value {value} is not a valid identifier",
	"validating 'txn:ERROR' gives an error" );
    is( check('ANY', 'pbdb:occ:ERROR'), "ERROR: Field {param}: the value {value} is not a valid " .
	"identifier", "and so does 'pbdb:occ:ERROR' for ANY" );
    is( scalar(extract_identifier('TXN', 'txn:ERROR')), undef, "extract_identifier of 'txn:ERROR'" );
};


# generate_identifier throws an exception for a type with several prefixes, which cannot
# be written as a single prefix, and for a type it does not know.

subtest 'types that cannot be generated' => sub {
    
    foreach my $type ( 'TID', 'OID', 'PLC' )
    {
	eval { generate_identifier($type, 5) };
	
	like( $@, qr{bad identifier type '$type'}, "generate_identifier for $type" );
    }
    
    foreach my $type ( 'XYZ', 'URN', 'UMS' )
    {
	eval { generate_identifier($type, 5) };
	
	like( $@, qr{bad identifier type '$type'}, "generate_identifier for $type" );
    }
    
    eval { generate_identifier(undef, 5) };
    
    like( $@, qr{bad identifier type ''}, "generate_identifier without a type" );
    
    eval { generate_identifier('TID', [1, 2]) };
    
    like( $@, qr{bad identifier type 'TID'}, "or for a list" );
    
    # The types that cannot be generated are found from %IDP, so a new type with more than
    # one prefix is included without being listed.
    
    is( join(' ', sort keys %ExternalIdent::DO_NOT_GENERATE),
	join(' ', sort keys %{{ map { $_ => 1 } 'URN', 'UMS', grep { $IDP{$_} =~ /[|]/ } keys %IDP }}),
	"every type with more than one prefix cannot be generated" );
};
