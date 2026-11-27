#
# PBDB Data Service
# -----------------
#
# lib-10-tabledata.t : Test complete_ruleset, add_to_ruleset, complete_output_block,
#                      and complete_valueset in TableData.pm, which build the rulesets,
#                      output blocks, and value sets of the data entry operations from
#                      the schemas of the tables. The tables are created in the test
#                      database, with one column of each type, and the data service is
#                      a stand-in.
#
# Added by Claude under the guidance of Michael McClennen.
#

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 9;

use CoreFunction qw(loadConfig connectDB);
use CoreTableDefs;
use TableDefs qw(%TABLE $TEST_DB init_table_names set_table_name set_column_property);
use HTTP::Validate qw(:validators);
use ExternalIdent qw(VALID_IDENTIFIER);

use TableData qw(complete_ruleset add_to_ruleset complete_output_block complete_valueset);


# A stand-in for the data service, which has a validator and keeps the documentation added
# to each output block.

package TableDataDS;

sub new { return bless { validator => HTTP::Validate->new, block => { }, set => { } }, $_[0] }

sub validator { $_[0]{validator} }

sub debug { 0 }

sub add_doc { push @{$_[0]{docs}{$_[1]{name}}}, $_[2] }

sub process_doc { $_[0]{processed}{$_[1]{name}} = 1 }


package main;


my $CONFIG = loadConfig();

init_table_names($CONFIG, 1);

my $dbh = connectDB(undef, "pbdb");

BAIL_OUT("no test database is configured") unless $TEST_DB;

my @TABLES;


# Create a table in the test database, with the specified specifier, name, and column
# definitions. Each test uses its own table, because the schemas are cached. The charset
# is utf8mb4, as for the real tables, whatever the default of the test database.

sub test_table {
    
    my ($specifier, $name, $columns) = @_;
    
    $dbh->do("CREATE OR REPLACE TABLE $TEST_DB.$name ($columns) DEFAULT CHARSET=utf8mb4");
    set_table_name($specifier => "$TEST_DB.$name");
    push @TABLES, "$TEST_DB.$name";
}

END { $dbh->do("DROP TABLE IF EXISTS $_") foreach @TABLES }


# Return a hash of the rules in the specified ruleset, keyed by parameter name.

sub rules {
    
    my ($ds, $ruleset) = @_;
    
    return map { $_->{param} => $_ } grep { $_->{param} } $ds->validator->list_rules($ruleset);
}

# Return the documentation of a rule, and its aliases.

sub doc { return ${$_[0]{doc_ref}} }

sub aliases { return ref $_[0]{alias} ? join(',', @{$_[0]{alias}}) : $_[0]{alias} // '' }


test_table('LIB10_TYPES', 'lib10_types', "lib10_no int unsigned primary key auto_increment,
	name varchar(40) not null default '', notes text character set latin1,
	tiny_note tinytext, raw_val varbinary(16), big_blob mediumblob, ratio float,
	dval double, price decimal(7,2) unsigned, shift_by decimal(5,1), flag tinyint(1),
	count smallint, big bigint unsigned, kind enum('a','b','c'), tags set('x','y'),
	born date, stamp timestamp null, loc point, bits bit(4)");


# Each column type is documented in the rule for its column.

subtest 'column types' => sub {
    
    my $ds = TableDataDS->new;
    
    $ds->validator->define_ruleset('types' => { optional => 'lib10_id' }, "The identifier.");
    
    complete_ruleset($ds, $dbh, 'types', 'LIB10_TYPES');
    
    my %r = rules($ds, 'types');
    
    ok( ! $r{lib10_no}, "a column with a parameter named like it in '_id' is left out" );
    
    like( doc($r{name}), qr{^This parameter sets the value of C<name> in the C<$TEST_DB.lib10_types> table[.]},
	  "the documentation names the column and the table" );
    like( doc($r{name}), qr{It accepts a string of maximum length 40[.]$}, "varchar" );
    like( doc($r{notes}), qr{It accepts a string of maximum length 65535}, "text" );
    like( doc($r{tiny_note}), qr{a string of maximum length 255}, "tinytext" );
    like( doc($r{ratio}), qr{a decimal value that can be stored in single precision floating point},
	  "float" );
    like( doc($r{price}), qr{an unsigned decimal value with at most 5 digits before the decimal and at most 2 digits after},
	  "unsigned decimal" );
    like( doc($r{shift_by}), qr{accepts a decimal value with at most 4 digits before}, "signed decimal" );
    like( doc($r{flag}), qr{any of the following values: C<'1', '0', 'true'}, "boolean" );
    like( doc($r{count}), qr{a two byte integer value}, "smallint" );
    like( doc($r{big}), qr{an unsigned eight byte integer value}, "unsigned bigint" );
    like( doc($r{kind}), qr{a value from the following list: C<'a', 'b', 'c'>}, "enum" );
    like( doc($r{tags}), qr{one or more values from the following list: C<'x', 'y'>}, "set" );
    like( doc($r{born}), qr{a date in any format acceptable to MariaDB}, "date" );
    like( doc($r{stamp}), qr{a timestamp in any format}, "timestamp" );
    like( doc($r{loc}), qr{a data type that is not reported}, "point" );
    like( doc($r{bits}), qr{a data type that is not reported}, "bit" );
    
    ok( $r{name}{allow_empty}, "each parameter allows an empty value" );
    ok( ! $r{name}{alias}, "and has no alias" );
    my $result = $ds->validator->check_params('types', { }, { flag => 'maybe' });
    ok( $result->errors, "which rejects a value that is not boolean" );
    
    $result = $ds->validator->check_params('types', { }, { flag => 'yes', name => 'abc' });
    ok( ! $result->errors, "and accepts one that is" );
  
    like( doc($r{dval}), qr{stored in double precision floating point},
	  "double is documented as double precision" );
    like( doc($r{notes}), qr{a string of maximum length 65535, stored as latin1[.]$},
	  "a latin1 text column names its charset" );
    unlike( doc($r{name}), qr{stored as}, "a utf8mb4 column does not" );
    like( doc($r{raw_val}), qr{It accepts binary data of maximum length 16[.]$}, "varbinary" );
    like( doc($r{big_blob}), qr{It accepts binary data of effectively unlimited length[.]$},
	  "mediumblob" );
    like( doc($r{born}), qr{acceptable to MariaDB[.]$}, "the documentation of a date has no newline" );
};


# The documentation given by 'doc' is added to the default, and that given by 'doc_full'
# replaces it. Each can contain placeholders that are filled in from the column type.

subtest 'documentation placeholders' => sub {
    
    my $ds = TableDataDS->new;
    
    $ds->validator->define_ruleset('docs' => { optional => 'lib10_id' }, "The identifier.");
    
    complete_ruleset($ds, $dbh, 'docs', 'LIB10_TYPES',
		     { name => { doc => 'It is %type of at most %size characters.' },
		       count => { doc_full => 'Holds %type of %size.' },
		       big => { doc_full => '%valid.' },
		       ratio => { doc_full => '%type, %size.' },
		       dval => { doc_full => '%size.' },
		       price => { doc_full => '%type, %size.' },
		       kind => { doc_full => '%type: %values.' },
		       tags => { doc_full => '%type: %values.' },
		       flag => { doc_full => '%type: %values.' },
		       born => { doc_full => '%type.' },
		       loc => { doc_full => 'T %type %size %values %charset %extid.' },
		       tiny_note => { doc_full => 'C %charset.' },
		       notes => { doc_full => 'C %charset.' },
		       raw_val => { doc_full => 'T %type %size %charset.' } });
    
    my %r = rules($ds, 'docs');
    
    like( doc($r{name}), qr{table[.] It is a string of at most 40 characters[.]$},
	  "'doc' is added to the default documentation" );
    is( doc($r{count}), 'Holds an integer of two bytes.', "'doc_full' replaces it" );
    is( doc($r{big}), 'an unsigned eight byte integer value.', "%valid" );
    is( doc($r{ratio}), 'a decimal number, single precision floating point.', "a float" );
    is( doc($r{dval}), 'double precision floating point.', "a double" );
    is( doc($r{price}), 'an unsigned decimal number, decimal(7,2).', "a decimal" );
    is( doc($r{kind}), "a value: 'a','b','c'.", "an enum" );
    is( doc($r{tags}), "one or more values: 'x','y'.", "a set" );
    is( doc($r{flag}), "a boolean value: '1','0','true','false','yes','no','y','n','on','off'.",
	"a boolean" );
    is( doc($r{born}), 'a date.', "a date" );
    is( doc($r{loc}), 'T ? ? ? ? ?.', "a type with no placeholder values" );
  
    is( doc($r{tiny_note}), 'C utf-8.', "%charset of a utf8mb4 column" );
    is( doc($r{notes}), 'C latin1.', "and of a latin1 column" );
    is( doc($r{raw_val}), 'T binary data 16 ?.', "%type, %size, and %charset of a binary column" );
};


test_table('LIB10_SKIP', 'lib10_skip', "skip_no int unsigned primary key auto_increment,
	authorizer_no int unsigned, enterer_no int unsigned, modifier_no int unsigned,
	authorizer varchar(20), enterer varchar(20), modifier varchar(20),
	created timestamp null, modified timestamp null, admin_lock tinyint(1),
	given varchar(20), hidden varchar(20), dropped varchar(20), dropped2 varchar(20),
	kept varchar(20)");

set_column_property('LIB10_SKIP', 'hidden', IGNORE => 1);


# The special columns, the columns for which the ruleset already has parameters, and the
# columns that are ignored are left out.

subtest 'columns left out' => sub {
    
    my $ds = TableDataDS->new;
    
    $ds->validator->define_ruleset('skip' => { optional => 'skip_id' }, "The identifier.",
				   { optional => 'given' }, "Given explicitly.");
    
    complete_ruleset($ds, $dbh, 'skip', 'LIB10_SKIP',
		     { dropped => { IGNORE => 1 }, dropped2 => 'IGNORE',
		       enterer => { doc => 'The enterer.' }, modified => { doc => 'Modified.' } });
    
    my %r = rules($ds, 'skip');
    
    is( join(',', sort keys %r), 'enterer,given,kept,modified,skip_id',
	"the parameters are the given ones, the ones overridden, and the ordinary column" );
    
    is( doc($r{given}), 'Given explicitly.', "a given parameter is not changed" );
    like( doc($r{enterer}), qr{The enterer[.]$}, "a name column can be included by an override" );
    like( doc($r{modified}), qr{Modified[.]$}, "and so can a special column" );
};


test_table('LIB10_PROPS', 'lib10_props', "prop_no int unsigned primary key auto_increment,
	ref_no int unsigned, coll_no int unsigned, title varchar(80), only_alt varchar(20),
	must varchar(20) not null, yes_no varchar(5), other_ref int unsigned, code varchar(10)");

set_column_property('LIB10_PROPS', 'ref_no', EXTID_TYPE => 'REF');
set_column_property('LIB10_PROPS', 'title', ALTERNATE_NAME => 'ref_title');
set_column_property('LIB10_PROPS', 'only_alt', ALTERNATE_NAME => 'alt_name');
set_column_property('LIB10_PROPS', 'only_alt', ALTERNATE_ONLY => 1);
set_column_property('LIB10_PROPS', 'must', REQUIRED => 1);


# The column properties and override attributes set the names, aliases, validators, and
# other attributes of the parameters.

subtest 'properties and overrides' => sub {
    
    my $ds = TableDataDS->new;
    
    $ds->validator->define_ruleset('props' => { optional => 'prop_id' }, "The identifier.",
				   { optional => 'only_alt' }, "The column name itself.");
    
    complete_ruleset($ds, $dbh, 'props', 'LIB10_PROPS',
		     { yes_no => { doc_valid => 'BOOLEAN_VALUE' },
		       other_ref => { doc_valid => 'VALID_IDENTIFIER(REF)' },
		       code => { alias => 'cd', doc => 'A code.' } });
    
    my %r = rules($ds, 'props');
    
    like( doc($r{ref_no}), qr{an external identifier of type 'ref', or an unsigned integer},
	  "EXTID_TYPE is documented" );
    
    my $result = $ds->validator->check_params('props', { }, { ref_no => 'ref:12' });
    ok( ! $result->errors, "and accepts an identifier of that type" );
    
    $result = $ds->validator->check_params('props', { }, { ref_no => 'col:12' });
    ok( $result->errors, "but not one of another type" );
    
    ok( $r{ref_title}, "ALTERNATE_NAME names the parameter" );
    is( aliases($r{ref_title}), 'title', "with the column name as an alias" );
    ok( ! $r{title}, "and no parameter named for the column" );
    
    ok( $r{alt_name}, "ALTERNATE_ONLY with a parameter named for the column adds the alternate" );
    ok( ! $r{alt_name}{alias}, "with no alias" );
    
    like( doc($r{must}), qr{This B<required> parameter}, "REQUIRED is documented" );
    
    like( doc($r{yes_no}), qr{It accepts any of the following values: C<'true', 'false'},
	  "doc_valid 'BOOLEAN_VALUE' is documented" );
    $result = $ds->validator->check_params('props', { }, { yes_no => 'maybe' });
    ok( $result->errors, "and sets a boolean validator" );
    
    like( doc($r{other_ref}), qr{an external identifier of type 'ref'},
	  "doc_valid 'VALID_IDENTIFIER(REF)' is documented" );
    $result = $ds->validator->check_params('props', { }, { other_ref => 'col:12' });
    ok( $result->errors, "and sets an identifier validator" );
    
    is( aliases($r{code}), 'cd', "other override attributes are set on the rule" );
    like( doc($r{code}), qr{A code[.]$}, "and not the documentation attributes" );
    ok( ! exists $r{code}{doc}, "which are not set on the rule" );
    
    eval { complete_ruleset($ds, $dbh, 'nosuch', 'LIB10_PROPS') };
    like( $@, qr{unknown ruleset 'nosuch'}, "an unknown ruleset" );
};


# add_to_ruleset adds rules with their documentation.

subtest 'add_to_ruleset' => sub {
    
    my $ds = TableDataDS->new;
    
    $ds->validator->define_ruleset('added' => { optional => 'first' }, "The first.");
    
    add_to_ruleset($ds, 'added', { optional => 'second' }, "The second", "parameter.",
		   { optional => 'third', valid => POS_VALUE });
    
    my %r = rules($ds, 'added');
    
    is( join(',', sort keys %r), 'first,second,third', "the rules were added" );
    is( doc($r{second}), "The second\nparameter.", "with their documentation" );
    
    my $result = $ds->validator->check_params('added', { }, { third => -1 });
    ok( $result->errors, "and their validators" );
    
    is( scalar @{$ds->{my_param_records}}, 2, "the rule records are kept" );
    
    eval { add_to_ruleset($ds, 'added', "no rule") };
    like( $@, qr{bad rule}, "documentation without a rule" );
};


test_table('LIB10_OUT', 'lib10_out', "out_no int unsigned primary key auto_increment,
	reference_no int unsigned, taxon_no int unsigned, specimen_no int unsigned,
	authorizer_no int unsigned, created timestamp null, title varchar(80),
	given varchar(20), hidden varchar(20), dropped varchar(20), dropped2 varchar(20),
	coded int, renamed varchar(20)");

set_column_property('LIB10_OUT', 'hidden', IGNORE => 1);


# complete_output_block adds an output field for each column, except for the special
# columns, the ones already in the block, and the ones ignored.

subtest 'output block' => sub {
    
    my $ds = TableDataDS->new;
    
    $ds->{block}{'out:basic'} = bless { name => 'out:basic', include_list => [ ],
					output_list => [ { output => 'given', com_name => 'giv' } ] },
	'Web::DataService::Block';
    
    complete_output_block($ds, $dbh, 'out:basic', 'LIB10_OUT',
			  { dropped => { IGNORE => 1 },
			    coded => { doc => 'A code.', com_name => 'cod' },
			    renamed => { com_name => undef } });
    
    my @list = @{$ds->{block}{'out:basic'}{output_list}};
    my %field = map { $_->{output} => $_ } grep { $_->{output} } @list;
    my %set = map { $_->{set} => $_ } grep { $_->{set} } @list;
    
    is( join(',', map { $_->{output} } grep { $_->{output} } @list),
	'given,out_no,reference_no,taxon_no,specimen_no,title,dropped2,coded,renamed',
	"the fields are the given one and the columns that are not left out" );
    
    is( $field{given}{com_name}, 'giv', "a given field is not changed" );
    is( $field{out_no}{com_name}, 'oid', "the first column in '_no' gets the compact name 'oid'" );
    is( $field{reference_no}{com_name}, 'reference_id', "a later one gets a name in '_id'" );
    is( $field{taxon_no}{com_name}, 'tid', "a common field gets its compact name" );
    is( $field{title}{com_name}, 'title', "another column gets its own name" );
    is( $field{coded}{com_name}, 'cod', "an override attribute is set" );
    ok( ! exists $field{renamed}{com_name}, "and one that is undefined is removed" );
    
    my @docs = grep { ! ref $_ } @{$ds->{docs}{'out:basic'}};
    
    ok( (grep { /^The contents of field C<title> from the table C<$TEST_DB.lib10_out>[.]$/ } @docs),
	"a field is documented" );
    ok( (grep { /field C<coded> .* The value will be an integer[.]$/ } @docs) == 0,
	"the documentation of an override replaces the default" );
    ok( (grep { $_ eq 'A code.' } @docs), "with the documentation given" );
    ok( (grep { /field C<specimen_no> .* The value will be an integer[.]$/ } @docs),
	"an integer field says so" );
    ok( $ds->{processed}{'out:basic'}, "the documentation is processed" );
    
    ok( $set{reference_no} && $set{specimen_no} && $set{taxon_no},
	"the identifier columns get a set rule" );
    
    my $code = $set{reference_no}{code};
    
    is( $code->({ block_hash => { extids => 1 } }, 12), 'ref:12',
	"which gives an external identifier with 'extids'" );
    is( $code->({ block_hash => { } }, 12), 12, "and the number without it" );
    is( $set{specimen_no}{code}->({ block_hash => { extids => 1 } }, 5), 'spm:5',
	"each type has its own code" );
    
    complete_output_block($ds, $dbh, 'out:new', 'LIB10_OUT');
    
    my $new = $ds->{block}{'out:new'};
    
    ok( $new && $new->isa('Web::DataService::Block'), "a block that is not yet defined is created" );
    is( (grep { $_->{output} && $_->{output} eq 'given' } @{$new->{output_list}}), 1,
	"with every field" );
    
    my %oid = map { $_->{output} => $_->{com_name} } grep { $_->{output} } @{$new->{output_list}};
    is( $oid{out_no}, 'oid', "a new block also gets 'oid'" );
    
    $ds->{block}{'out:oid'} = bless { name => 'out:oid', include_list => [ ],
				      output_list => [ { output => 'x', com_name => 'oid' } ] },
	'Web::DataService::Block';
    
    complete_output_block($ds, $dbh, 'out:oid', 'LIB10_OUT');
    
    %oid = map { $_->{output} => $_->{com_name} } grep { $_->{output} } @{$ds->{block}{'out:oid'}{output_list}};
    is( $oid{out_no}, 'out_id', "a block that already has 'oid' does not get another" );
  
    complete_output_block($ds, $dbh, 'out:ignore', 'LIB10_OUT', { dropped2 => 'IGNORE' });
    
    ok( ! (grep { $_->{output} && $_->{output} eq 'dropped2' }
	   @{$ds->{block}{'out:ignore'}{output_list}}), "a column overridden with 'IGNORE' is left out" );
};


test_table('LIB10_VALUES', 'lib10_values', "value_no int unsigned primary key, label varchar(20)");

$dbh->do("INSERT INTO $TABLE{LIB10_VALUES} VALUES (1, 'one'), (2, 'two'), (3, 'three')");


# complete_valueset adds the numbers and strings from a table to a set.

subtest 'complete_valueset' => sub {
    
    my $ds = TableDataDS->new;
    
    $ds->{set}{'values'} = { value_list => [ 'zero' ], value => { zero => 0, 0 => 'zero' } };
    
    complete_valueset($ds, $dbh, 'values', 'LIB10_VALUES');
    
    my $vs = $ds->{set}{'values'};
    
    is( join(',', @{$vs->{value_list}}), 'zero,one,two,three', "the strings are added to the list" );
    is( join(',', @{$vs->{numeric_list}}), '1,2,3', "the numbers are listed" );
    is( $vs->{value}{two}, 2, "a string maps to its number" );
    is( $vs->{value}{3}, 'three', "and a number to its string" );
    
    eval { complete_valueset($ds, $dbh, 'nosuch', 'LIB10_VALUES') };
    like( $@, qr{unknown set 'nosuch'}, "an unknown set" );
    
    eval { complete_valueset($ds, $dbh, 'values', 'LIB10_NOSUCH') };
    like( $@, qr{unknown table 'LIB10_NOSUCH'}, "an unknown table" );
};


# The tables are all in the test database.

subtest 'test database' => sub {
    
    foreach my $t ( @TABLES )
    {
	like( $t, qr{^$TEST_DB[.]}, "$t is in the test database" );
    }
};


# The parameter records are kept by the data service, because the validator weakens its
# references to them.

subtest 'parameter records' => sub {
    
    my $ds = TableDataDS->new;
    
    $ds->validator->define_ruleset('kept' => { optional => 'prop_id' }, "The identifier.");
    
    complete_ruleset($ds, $dbh, 'kept', 'LIB10_PROPS');
    
    my %r = rules($ds, 'kept');
    
    is( scalar @{$ds->{my_param_records}}, scalar(keys %r) - 1,
	"a record is kept for each parameter added" );
};
