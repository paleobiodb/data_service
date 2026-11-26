#
# Paleobiology Database library modules
# -------------------------------------
#
# This file contains unit tests for library modules that are used by the EditTransaction
# client modules and by the data service.
#
# lib-08-definitions.t : Check the definition modules CoreTableDefs.pm, ResourceDefs.pm,
# PBDBFields.pm, and TaxonDefs.pm against the database and against the code that uses
# them: that every table they name exists, that every column they name exists, that every
# table or type they refer to is defined, and that every value they give is one the code
# understands.
#
# These checks go through whatever the modules define when the test is run, so a new
# definition is checked without any change here. Only the lists of accepted values that
# cannot be read from the code, and the exceptions, are kept below.
#
# The database is read but not written, and no tables are switched to the test database.
#
# Added by Claude under the guidance of Michael McClennen.
#

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 7;

use File::Find;

use CoreFunction qw(connectDB);
use TableDefs qw(%TABLE get_table_properties);
use CoreTableDefs;
use ResourceDefs;
use PBDBFields qw(%COMMON_FIELD_SPECIAL %COMMON_FIELD_IDTYPE %FOREIGN_KEY_TABLE %FOREIGN_KEY_COL);
use TaxonDefs qw(%TAXON_RANK %RANK_STRING %UNS_NAME %UNS_RANK %TAXON_TABLE @TREE_TABLE_LIST
		 %TAXONOMIC_STATUS %NOMENCLATURAL_STATUS);
use ExternalIdent qw(%IDP);

# A client module is loaded so that the directive names registered by the PaleoBioDB
# module are available.

use PBDBEdit;


my $dbh = connectDB('config.yml');

my ($DEFAULT_DB) = $dbh->selectrow_array("SELECT database()");


# The value of each permission property must be one that Permissions.pm or the
# EditTransaction module PaleoBioDB acts on. These lists are kept here because those
# modules have no list to read them from. 'ADMIN' and 'NONE' are not matched by any rule,
# so that only administrators (or no one) have the permission.

my %ACCEPTED = ( CAN_POST => [ qw(LOGGED_IN MEMBERS AUTHORIZED NOT_STUDENT ALL ANY ADMIN NONE) ],
		 CAN_VIEW => [ qw(LOGGED_IN MEMBERS AUTHORIZED NOT_STUDENT ALL ANY ADMIN NONE) ],
		 CAN_MODIFY => [ qw(LOGGED_IN MEMBERS AUTHORIZED NOT_STUDENT ALL ANY ADMIN NONE) ],
		 CAN_DELETE => [ qw(OWNER AUTHORIZED ADMIN NONE) ],
		 CAN_MOVE => [ qw(ALL ANY OWNER NONE) ] );


# The names of every table and column in the databases that the definitions use.

my (%TABLE_EXISTS, %COLUMN_EXISTS);

foreach my $r ( @{$dbh->selectall_arrayref("SELECT table_schema, table_name, column_name
		FROM information_schema.columns")} )
{
    $TABLE_EXISTS{"$r->[0].$r->[1]"} = 1;
    $COLUMN_EXISTS{"$r->[0].$r->[1]"}{$r->[2]} = 1;
}


# Return the full name of a table, with the default database if it has none.

sub full_name { my ($name) = @_; return $name =~ /[.]/ ? $name : "$DEFAULT_DB.$name" }


# Return the specifiers in %TABLE, leaving out the copies of original names that
# change_table_db makes.

sub specifiers { return grep { ! /^==/ } sort keys %TABLE }


# Run a check over a list of items. Each failing item is reported in the description of a
# single test, so the test passes only if none fail. Items whose key is in the %todo hash
# are checked separately as TODO tests, with the reason given.

sub check_all {
    
    my ($label, $items, $check, $todo) = @_;
    
    $todo ||= { };
    
    my @failed = grep { ! $todo->{$_} && ! $check->($_) } @$items;
    
    ok( ! @failed, $label ) || diag("failed: @failed");
    
    foreach my $item ( sort keys %$todo )
    {
	next unless grep { $_ eq $item } @$items;
      
      TODO: {
	    local $TODO = $todo->{$item};
	    
	    ok( $check->($item), "$label: $item" );
	}
    }
}


# Every table named in %TABLE exists in the database.

subtest 'tables' => sub {
    
    my @specs = specifiers();
    
    ok( @specs > 50, "there are many table definitions" );
    
    check_all( "every table exists", \@specs,
	       sub { $TABLE_EXISTS{full_name($TABLE{$_[0]})} } );
};


# Every table with properties is defined, and every property that names columns or tables
# names ones that exist.

subtest 'table properties' => sub {
    
    check_all( "every table with properties is defined", [ sort keys %TableDefs::TABLE_PROPERTIES ],
	       sub { exists $TABLE{$_[0]} } );
    
    my (@column_props, @table_props, @value_props);
    
    foreach my $t ( specifiers() )
    {
	my %p = get_table_properties($t);
	
	foreach my $prop ( qw(PRIMARY_KEY AUTH_FIELDS REQUIRED_COLS) )
	{
	    next unless $p{$prop};
	    push @column_props, map { "$t:$prop:$_" } ref $p{$prop} ? @{$p{$prop}} : split /\s*,\s*/, $p{$prop};
	}
	
	foreach my $prop ( qw(AUTH_TABLE CASCADE_DELETE) )
	{
	    next unless $p{$prop};
	    push @table_props, map { "$t:$prop:$_" } ref $p{$prop} ? @{$p{$prop}} : $p{$prop};
	}
	
	foreach my $prop ( sort keys %ACCEPTED )
	{
	    push @value_props, "$t:$prop:$p{$prop}" if defined $p{$prop};
	}
    }
    
    check_all( "every column named by PRIMARY_KEY, AUTH_FIELDS, or REQUIRED_COLS exists", \@column_props,
	       sub { my ($t, $prop, $c) = split /:/, $_[0];
		     my $f = full_name($TABLE{$t});
		     ! $TABLE_EXISTS{$f} || $COLUMN_EXISTS{$f}{$c} } );
    
    check_all( "every table named by AUTH_TABLE or CASCADE_DELETE is defined", \@table_props,
	       sub { my ($t, $prop, $x) = split /:/, $_[0]; exists $TABLE{$x} } );
    
    check_all( "every permission property has a value the code acts on", \@value_props,
	       sub { my ($t, $prop, $v) = split /:/, $_[0]; grep { $_ eq uc $v } @{$ACCEPTED{$prop}} } );
};


# Every column with properties exists, except one whose only property is IGNORE, which is
# meant for fields that are not columns. Every FOREIGN_KEY names a defined table, and
# every EXTID_TYPE is a known identifier type.

subtest 'column properties' => sub {
    
    my (@columns, @fkeys, @extids);
    
    foreach my $t ( sort keys %TableDefs::COLUMN_PROPERTIES )
    {
	foreach my $c ( sort keys %{$TableDefs::COLUMN_PROPERTIES{$t}} )
	{
	    my $p = $TableDefs::COLUMN_PROPERTIES{$t}{$c};
	    
	    push @columns, "$t:$c" unless join(',', keys %$p) eq 'IGNORE';
	    push @fkeys, "$t:$c:$p->{FOREIGN_KEY}" if $p->{FOREIGN_KEY};
	    push @extids, "$t:$c:$p->{EXTID_TYPE}" if $p->{EXTID_TYPE};
	}
    }
    
    check_all( "every table with column properties is defined",
	       [ sort keys %TableDefs::COLUMN_PROPERTIES ], sub { exists $TABLE{$_[0]} } );
    
    check_all( "every column with properties exists", \@columns,
	       sub { my ($t, $c) = split /:/, $_[0];
		     my $f = full_name($TABLE{$t});
		     ! $TABLE_EXISTS{$f} || $COLUMN_EXISTS{$f}{$c} } );
    
    check_all( "every FOREIGN_KEY names a defined table", \@fkeys,
	       sub { my ($t, $c, $x) = split /:/, $_[0]; exists $TABLE{$x} } );
    
    check_all( "every EXTID_TYPE is a known identifier type", \@extids,
	       sub { my ($t, $c, $x) = split /:/, $_[0]; $IDP{$x} } );
};


# Every table in a group is defined, and every group is used by some code outside the
# definition modules.

subtest 'table groups' => sub {
    
    my @groups = sort keys %TableDefs::TABLE_GROUP;
    
    check_all( "every table in a group is defined",
	       [ map { my $g = $_; map { "$g:$_" } @{$TableDefs::TABLE_GROUP{$g}} } @groups ],
	       sub { my ($g, $t) = split /:/, $_[0]; exists $TABLE{$t} } );
    
    # A group is used if its name appears as a word in a module, script, or test file
    # other than the ones that define the groups, and this one. Groups are also enabled by
    # name through the data service operation that PBMain.pm handles, so the name may be
    # part of a URL path.
    
    my $source = '';
    
    find( sub { return unless /[.](pm|t|pl)$/ && ! /~$/;
		return if $File::Find::name =~ m{ /(CoreTableDefs|ResourceDefs|TableDefs)[.]pm$ }x;
		return if $File::Find::name =~ m{ /lib-08-definitions[.]t$ }x;
		open my $fh, '<', $_ or return; local $/; $source .= <$fh> },
	  'lib', 't', 'bin' );
    
    # These groups are not used in this repository, but are kept on purpose: any group can
    # be enabled through the data service's testmode request.
    
    my %kept = map { $_ => 1 } qw(authority_data collection_summary country_data diversity_data
				 paleocoord_data prevalence_data reference_data specimen_elements);
    
    check_all( "every group is used, except those kept on purpose", [ grep { ! $kept{$_} } @groups ],
	       sub { my $g = quotemeta $_[0]; $source =~ /\b$g\b/ } );
    
    check_all( "every group kept on purpose is defined", [ sort keys %kept ],
	       sub { $TableDefs::TABLE_GROUP{$_[0]} } );
};


# PBDBFields: every foreign key table is defined, every foreign key column exists, every
# identifier type can be generated, and every special field is a known directive.

subtest 'PBDBFields' => sub {
    
    check_all( "every table in %FOREIGN_KEY_TABLE is defined", [ sort keys %FOREIGN_KEY_TABLE ],
	       sub { exists $TABLE{$FOREIGN_KEY_TABLE{$_[0]}} } );
    
    check_all( "every column in %FOREIGN_KEY_COL exists in its table", [ sort keys %FOREIGN_KEY_COL ],
	       sub { my $t = $FOREIGN_KEY_TABLE{$_[0]};
		     my $f = full_name($TABLE{$t});
		     $COLUMN_EXISTS{$f}{$FOREIGN_KEY_COL{$_[0]}} } );
    
    check_all( "every type in %COMMON_FIELD_IDTYPE can be generated", [ sort keys %COMMON_FIELD_IDTYPE ],
	       sub { my $type = $COMMON_FIELD_IDTYPE{$_[0]};
		     $IDP{$type} && ! $ExternalIdent::DO_NOT_GENERATE{$type} } );
    
    my %directive = map { %$_ } values %EditTransaction::Directives::DIRNAME_BY_CLASS;
    
    check_all( "every value in %COMMON_FIELD_SPECIAL is a registered directive",
	       [ sort keys %COMMON_FIELD_SPECIAL ], sub { $directive{$COMMON_FIELD_SPECIAL{$_[0]}} } );
};


# TaxonDefs: the rank maps agree, every unspecified-taxon code has a rank, every status
# has a Darwin Core equivalent, and the taxonomy tables exist.

subtest 'TaxonDefs' => sub {
    
    check_all( "every rank in %RANK_STRING maps back to its number in %TAXON_RANK",
	       [ sort { $a <=> $b } keys %RANK_STRING ],
	       sub { $TAXON_RANK{$RANK_STRING{$_[0]}} == $_[0] } );
    
    check_all( "every numbered rank in %TAXON_RANK has a name in %RANK_STRING",
	       [ grep { /^\d+$/ } sort keys %TAXON_RANK ], sub { $RANK_STRING{$_[0]} } );
    
    check_all( "every code in %UNS_NAME has a rank in %UNS_RANK", [ sort keys %UNS_NAME ],
	       sub { defined $UNS_RANK{$_[0]} } );
    
    check_all( "every code in %UNS_RANK has a name in %UNS_NAME", [ sort keys %UNS_RANK ],
	       sub { $UNS_NAME{$_[0]} } );
    
    my @all_status = $TaxonDefs::ALL_STATUS =~ /'([^']+)'/g;
    
    foreach my $list ( qw(VARIANT_STATUS INVALID_STATUS JUNIOR_STATUS) )
    {
	no strict 'refs';
	
	my @statuses = ${"TaxonDefs::$list"} =~ /'([^']+)'/g;
	
	check_all( "every status in \$$list is in \$ALL_STATUS", \@statuses,
		   sub { grep { $_ eq $_[0] } @all_status } );
    }
    
    check_all( "every status other than 'belongs to' has a nomenclatural status",
	       [ grep { $_ ne 'belongs to' && $_ ne 'subjective synonym of' &&
			    $_ ne 'objective synonym of' } sort keys %TAXONOMIC_STATUS ],
	       sub { $NOMENCLATURAL_STATUS{$_[0]} } );
    
    check_all( "every taxonomy table exists",
	       [ map { my $tree = $_; ($tree, values %{$TAXON_TABLE{$tree}}) } @TREE_TABLE_LIST ],
	       sub { $TABLE_EXISTS{full_name($_[0])} } );
};


# A few fixed values, to show that the definitions are loaded.

subtest 'spot checks' => sub {
    
    is( $TABLE{REFERENCE_DATA}, 'refs', "REFERENCE_DATA is 'refs'" );
    is( TableDefs::get_table_property('REFERENCE_DATA', 'PRIMARY_KEY'), 'reference_no',
	"its primary key" );
    is( $TAXON_RANK{genus}, 5, "the rank of a genus" );
    is( $FOREIGN_KEY_TABLE{taxon_no}, 'AUTHORITY_DATA', "the foreign key table of taxon_no" );
};
