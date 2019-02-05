# 
# TableData.pm
# 
# This module manages table schemas. It fetches them when necessary and checks records against
# them to make sure that data inserts and updates will complete properly.
# 
# Author: Michael McClennen

package TableData;

use strict;

use TableDefs qw(%TABLE get_table_property get_column_properties list_column_properties
		 %COMMON_FIELD_IDTYPE %COMMON_FIELD_SPECIAL);

use Carp qw(croak);
use ExternalIdent qw(extract_identifier generate_identifier VALID_IDENTIFIER);

use base 'Exporter';

our (@EXPORT_OK) = qw(complete_output_block complete_ruleset
		      get_table_schema reset_cached_column_properties get_authinfo_fields);

our (@CARP_NOT) = qw(EditTransaction Try::Tiny);

our (%COMMON_FIELD_COM) = ( taxon_no => 'tid',
			    resource_no => 'rid',
			    collection_no => 'cid',
			    interval_no => 'iid',
			    authorizer_no => 'ati',
			    enterer_no => 'eni',
			    modifier_no => 'mdi',
			    created => 'dcr',
			    modified => 'dmd',
			  );

our (%COMMON_FIELD_IDSUB);

our (%SCHEMA_CACHE);

our (@SCHEMA_COLUMN_PROPS) = qw(REQUIRED ALTERNATE_NAME ALTERNATE_ONLY ALLOW_TRUNCATE VALUE_SEPARATOR
				ADMIN_SET FOREIGN_TABLE FOREIGN_KEY EXTID_TYPE VALIDATOR IGNORE);

our (%PREFIX_SIZE) = ( tiny => 255,
		       regular => 65535,
		       medium => 16777215,
		       large => 4294967295 );

our (%SIGNED_BOUND) = ( tiny => 127,
			small => 32767,
			medium => 8388607,
			regular => 2147483647,
			big => 9223372036854775807 );

our (%UNSIGNED_BOUND) = ( tiny => 255,
			  small => 65535,
			  medium => 16777215,
			  regular => 4294967295,
			  big => 18446744073709551615 );


# get_table_scheme ( table_name, debug_flag )
# 
# Fetch the schema for the specified table, and return it as a hash ref. This information is
# cached, so that subsequent queries can be satisfied without hitting the database again. The key
# '_column_list' contains a list of the column names, in the order they appear in the table.

sub get_table_schema {
    
    my ($dbh, $table_specifier, $debug) = @_;
    
    # If we already have the schema cached, just return it.
    
    return $SCHEMA_CACHE{$table_specifier} if ref $SCHEMA_CACHE{$table_specifier} eq 'HASH';
    
    # Otherwise construct an SQL statement to get the schema from the appropriate database.

    my $table_name;

    if ( $table_specifier =~ /^==(.*)/ )
    {
	croak "Unknown table '$table_specifier'" unless exists $TABLE{$table_specifier} && $TABLE{$table_specifier};
	
	$table_name = $TABLE{$table_specifier};
	$table_specifier = $1;
	# $table_name =~ s/^\w+[.]//;
    }
    
    else
    {
	croak "Unknown table '$table_specifier'" unless exists $TABLE{$table_specifier} && $TABLE{$table_specifier};
	
	$table_name = $TABLE{$table_specifier};
    }
    
    my ($sql, $check_table, %schema, $quoted_table);
    
    if ( $table_name =~ /(\w+)[.](.+)/ )
    {
	$sql = "SHOW TABLES FROM `$1` LIKE " . $dbh->quote($2);
	$quoted_table = "`$1`.". $dbh->quote_identifier($2);
    }
    
    else
    {
	$sql = "SHOW TABLES LIKE " . $dbh->quote($table_name);
	$quoted_table = $dbh->quote_identifier($table_name);
    }
    
    print STDERR "$sql\n\n" if $debug;
    
    eval {
	($check_table) = $dbh->selectrow_array($sql);
    };
    
    croak "unknown table '$table_specifier'" unless $check_table;
    
    print STDERR "	SHOW COLUMNS FROM $quoted_table\n\n" if $debug;
    
    my $columns_ref = $dbh->selectall_arrayref("
	SHOW COLUMNS FROM $quoted_table", { Slice => { } });
    
    # Figure out which columns from this table have had properties set for them.
    
    my %has_properties = list_column_properties($table_specifier);
    
    # Now go through the columns one by one. Find the primary key if there is one, and also parse
    # the column datatypes. Collect up the list of field names for easy access later.
    
    my @field_list;
    
    foreach my $c ( @$columns_ref )
    {
	# Each field definition comes to us as a hash. The name is in 'Field'.
	
	my $field = $c->{Field};
	
	$schema{$field} = $c;
	push @field_list, $field;
	
	# if ( $c->{Key} =~ 'PRI' && ! $schema{_primary} )
	# {
	#     $schema{_primary} = $field;
	# }
    }
    
    # Then go through the list again and add the proper attributes to each column.

    foreach my $c ( @$columns_ref )
    {
	my $field = $c->{Field};
	
	# If the column has properties, then record those we are interested in.
	
	if ( $has_properties{$field} )
	{
	    my %properties = get_column_properties($table_specifier, $field);
	    
	    foreach my $p ( @SCHEMA_COLUMN_PROPS )
	    {
		$c->{$p} = $properties{$p} if defined $properties{$p};
	    }

	    if ( ref $c->{VALIDATOR} && ref $c->{VALIDATOR} ne 'code' )
	    {
		croak "the value of VALIDATOR must be a code ref";
	    }
	}
	
	# If the column is Not Null and has neither a default value nor auto_increment, then mark
	# it as REQUIRED. Otherwise, a database error will be generated when we try to insert or
	# update a record with a null value for this column. But not if the column type is BLOB or
	# TEXT, because of an issue with MariaDB 10.0-10.1.
	
	if ( $c->{Null} && $c->{Null} eq 'NO' && not ( defined $c->{Default} ) &&
	     not ( $c->{Extra} && $c->{Extra} =~ /auto_increment/i ) )
	{
	    $c->{REQUIRED} = 1 unless $c->{Type} =~ /blob|text/i;
	}
	
	# If the name of the field ends in _no, then record its alternate as the same name with
	# _id substituted unless there is already a field with that name.
	
	if ( ! $c->{ALTERNATE_NAME} && $field =~ qr{ ^ (.*) _no }xs )
	{
	    my $alt = $1 . '_id';
	    
	    unless ( $schema{$alt} )
	    {
		$c->{ALTERNATE_NAME} = $alt;
	    }
	}
	
	# The type definition is in 'Type'. We parse each type, for easy access by validation
	# routines later, and store the parsed values in TypeParams.
	
	my $type = $c->{Type};
	
	if ( $type =~ qr{ ^ ( var )? ( char | binary ) [(] ( \d+ ) }xs )
	{
	    my $type = $2 eq 'char' ? 'text' : 'data';
	    my $mode = $1 ? 'variable' : 'fixed';
	    $c->{TypeParams} = [ $type, $3, $mode ];
	}
	
	elsif ( $type =~ qr{ ^ ( tiny | medium | long )? ( text | blob ) (?: [(] ( \d+ ) )? }xs )
	{
	    my $type = $2 eq 'text' ? 'text' : 'data';
	    my $size = $3 || $PREFIX_SIZE{$1 || 'regular'};
	    $c->{TypeParams} = [ $type, $size, 'variable' ];
	}
	
	elsif ( $type =~ qr{ ^ tinyint [(] 1 [)] }xs )
	{
	    $c->{TypeParams} = [ 'boolean' ];
	}
	
	elsif ( $type =~ qr{ ^ (tiny|small|medium|big)? int [(] (\d+) [)] \s* (unsigned)? }xs )
	{
	    my $bound = $3 ? $UNSIGNED_BOUND{$1 || 'regular'} : $SIGNED_BOUND{$1 || 'regular'};
	    my $unsigned = $3 ? 'unsigned' : '';
	    $c->{TypeParams} = [ 'integer', $unsigned, $bound, $2 ];
	}
	
	elsif ( $type =~ qr{ ^ decimal [(] (\d+) , (\d+) [)] \s* (unsigned)? }xs )
	{
	    my $unsigned = $3 ? 'unsigned' : '';
	    my $before = $1 - $2;
	    my $after = $2;
	    $after = 10 if $after > 10;	# This is necessary for value checking in EditTransaction.pm.
					# If people want fields with more than 10 decimals, they should
					# use floating point.
	    $c->{TypeParams} = [ 'fixed', $unsigned, $before, $after ];
	}
	
	elsif ( $type =~ qr{ ^ ( float | double ) (?: [(] ( \d+ ) , ( \d+ ) [)] )? \s* (unsigned)? }xs )
	{
	    my $unsigned = $3 ? 'unsigned' : '';
	    my $precision = $1;
	    my $before = defined $2 ? $2 - $3 : undef;
	    my $after = $3;
	    $c->{TypeParams} = [ 'floating', $unsigned, $precision, $before, $after ];
	}
	
	elsif ( $type =~ qr{ ^ bit [(] ( \d+ ) }xs )
	{
	    $c->{TypeParams} = [ 'bits', $1 ];
	}
	
	elsif ( $type =~ qr{ ^ ( enum | set ) [(] (.+) [)] $ }xs )
	{
	    my $type = $1;
	    my $value_hash = unpack_enum($2);
	    $c->{TypeParams} = [ $type, $value_hash ];
	}
	
	elsif ( $type =~ qr{ ^ ( date | time | datetime | timestamp ) \b }xs )
	{
	    $c->{TypeParams} = [ 'date', $1 ];
	}
	
	elsif ( $type =~ qr{ ^ ( (?: multi )? (?: point | linestring | polygon ) ) \b }xs )
	{
	    $c->{TypeParams} = [ 'geometry', $1 ];
	}
	
	elsif ( $type =~ qr{ ^ ( geometry (?: collection )? ) \b }xs )
	{
	    $c->{TypeParams} = [ 'geometry', $1 ];
	}

	else
	{
	    $c->{TypeParams} = [ 'unknown' ];
	}
    }
    
    $schema{_column_list} = \@field_list;
    
    $SCHEMA_CACHE{$table_specifier} = \%schema;
    
    return \%schema;
}


# unpack_enum ( value_string )
# 
# Given a string of values, unpack them and construct a hash ref with each value as a key.

sub unpack_enum {

    use feature 'fc';
    
    my ($string) = @_;
    
    my $value_hash = { };
    
    while ( $string =~ qr{ ^ ['] ( (?: [^'] | '' )* ) ['] ,? (.*) }xs )
    {
	my $value = $1;
	$string = $2;

	$value =~ s/''/'/g;
	$value_hash->{fc $value} = 1;
    }
    
    if ( $string )
    {
	print STDERR "ERROR: could not parse ENUM($_[0])";
    }
    
    return $value_hash;
}


# reset_cached_column_properties ( table_name, column_name )
#
# This routine is intended primarily for for testing purposes.

sub reset_cached_column_properties {
    
    my ($table_specifier, $column_name) = @_;
    
    if ( my $col = $SCHEMA_CACHE{$table_specifier}{$column_name} )
    {
	my %properties = get_column_properties($table_specifier, $column_name);
	
	foreach my $p ( @SCHEMA_COLUMN_PROPS )
	{
	    delete $col->{$p};
	    $col->{$p} = $properties{$p} if defined $properties{$p};
	}
	
	# If the column is Not Null and has neither a default value nor auto_increment, then mark it
	# as REQUIRED. Otherwise, a database error will be generated when we try to insert or
	# update a record with a null value for this column.
	
	if ( $col->{Null} && $col->{Null} eq 'NO' && not ( defined $col->{Default} ) &&
	     not ( $col->{Extra} && $col->{Extra} =~ /auto_increment/i ) )
	{
	    $col->{REQUIRED} = 1;
	}
	
	# If the name of the field ends in _no, then record its alternate as the same name with
	# _id substituted unless there is already a field with that name.
	
	if ( ! $col->{ALTERNATE_NAME} && $column_name =~ qr{ ^ (.*) _no }xs )
	{
	    my $alt = $1 . '_id';
	    $col->{ALTERNATE_NAME} = $alt;
	}
    }
}


# get_authinfo_fields ( dbh, table_name, debug )
# 
# Return a list of the fields from the specified table that record who created each record. If
# there are none, return false.

our (%IS_AUTH) = (authorizer_no => 1, enterer_no => 1, enterer_id => 1, admin_lock => 1, owner_lock => 1);
our (%AUTH_FIELD_CACHE);

sub get_authinfo_fields {

    my ($dbh, $table_specifier, $debug) = @_;
    
    # If we already have this info cached, just return it.
    
    return $AUTH_FIELD_CACHE{$table_specifier} if exists $AUTH_FIELD_CACHE{$table_specifier};
    
    # Otherwise, get a hash of table column definitions
    
    my $schema = get_table_schema($dbh, $table_specifier, $debug);
    
    # If we don't have one, then barf.
    
    unless ( $schema && $schema->{_column_list} )
    {
	croak "Cannot retrieve schema for table '$table_specifier'";
    }
    
    # Then scan through the columns and collect up the names that are significant.
    
    my @authinfo_fields;
    
    foreach my $col ( @{$schema->{_column_list}} )
    {
	push @authinfo_fields, $col if $IS_AUTH{$col};
    }
    
    my $fields = join(', ', @authinfo_fields);
    $AUTH_FIELD_CACHE{$table_specifier} = $fields;
    
    return $fields;
}


sub complete_output_block {
    
    my ($ds, $dbh, $block_name, $table_specifier) = @_;
    
    # First get a hash of table column definitions
    
    my $schema = get_table_schema($dbh, $table_specifier, $ds->debug);
    
    # Then get the existing contents of the block and create a hash of the field names that are
    # already defined. If no block by this name is yet defined, create an empty one.
    
    unless ( $ds->{block}{$block_name} )
    {
	my $new_block = { name => $block_name,
			  include_list => [],
			  output_list => [] };
	
	$ds->{block}{$block_name} = bless $new_block, 'Web::DataService::Block';
    }
    
    my $block = $ds->{block}{$block_name};
    my $output_list = $block->{output_list};
    my %block_has_field;
    my $block_needs_oid = 1;
    
    foreach my $b ( @$output_list )
    {
	$block_has_field{$b->{output}} = 1 if $b->{output};
	$block_needs_oid = 0 if $b->{com_name} && $b->{com_name} eq 'oid';
    }
    
    # Then go through the field list from the schema and add any fields that aren't already in the
    # output list. We need to translate names that end in '_no' to '_id', and we can substitute
    # compact vocabulary names where known.
    
    my $field_list = $schema->{_column_list};
    
    foreach my $field_name ( @$field_list )
    {
	# If this field is one of the standard ones for authorizer/enterer or created/modified,
	# then skip it.
	
	if ( $COMMON_FIELD_SPECIAL{$field_name} )
	{
	    next;
	}
	
	# If this field is already in the output block, skip it as well. This allows us to
	# explicitly include some of the fields in the block definition, with documentation
	# strings and other attributes, and prevents duplicate output fields.
	
	next if $block_has_field{$field_name};
	
	# If this field has the 'IGNORE' attribute set, skip it as well.

	next if $schema->{$field_name}{IGNORE};
	
	# Now create a record to represent this field, along with a documentation string and
	# whatever other attributes we can glean from the table definition.
	
	my $field_record = $schema->{$field_name};
	my $type = $field_record->{Type};
	
	my $r = { output => $field_name };
	
	if ( $COMMON_FIELD_COM{$field_name} )
	{
	    $r->{com_name} = $COMMON_FIELD_COM{$field_name};
	}
	
	elsif ( $field_name =~ /(.*)_no/ )	# $$$ need to replace this with a hash mapping _no
                                                # => _id
	{
	    if ( $block_needs_oid )
	    {
		$r->{com_name} = 'oid';
	    }
	    
	    else
	    {
		$r->{com_name} = $1 . '_id';
	    }
	}
	
	else
	{
	    $r->{com_name} = $field_name;
	}
	
	my $doc = "The contents of field C<$field_name> from the table.";
	
	if ( $type =~ /int\(/ )
	{
	    $doc .= " The value will be an integer.";
	}
	
	$block_needs_oid = 0;
	
	push @$output_list, $r;
	$ds->add_doc($block, $r);
	$ds->add_doc($block, $doc);
	
	# If the field is one that we know contains a value that should be expressed as an
	# external identifier, create a subroutine to do that.
	
	if ( my $type = $COMMON_FIELD_IDTYPE{$field_name} )
	{
	    unless ( $COMMON_FIELD_IDSUB{$type} )
	    {
		$COMMON_FIELD_IDSUB{$type} = sub {
		    my ($request, $value) = @_;
		    return $value unless $request->{block_hash}{extids};
		    return generate_identifier($type, $value);
		};
	    }
	    
	    push @$output_list, { set => $field_name, code => $COMMON_FIELD_IDSUB{$type} };
	}
    }
    
    $ds->process_doc($block);
}


# complete_ruleset ( dbh, ruleset_name, table_specifier )
#
# Complete the definition of the specified ruleset by reading the table schema corresponding to
# $table_identifier and iterating through the columns. For each table column, add a new parameter
# to the ruleset unless an existing ruleset parameter already corresponds to that column.

sub complete_ruleset {
    
    my ($ds, $dbh, $ruleset_name, $table_specifier, @definitions) = @_;
    
    # First get a hash of table column definitions
    
    my $schema = get_table_schema($dbh, $table_specifier, $ds->debug);
    
    # Then get the existing ruleset documentation and create a hash of the field names that are
    # already defined. If no ruleset by this name is yet defined, croak.
    
    my $rs = $ds->validator->{RULESETS}{$ruleset_name};
    
    croak "unknown ruleset '$ruleset_name'" unless defined $rs;
    
    my @param_list = $ds->validator->list_params($ruleset_name);
    
    my %ruleset_has_field = map { $_ => 1 } @param_list;
    
    # We need to keep a list of the parameter records generated below, because the references to
    # them inside the validator record are weakened.
    
    $ds->{my_param_records} ||= [ ];
    
    # Then go through the field list from the schema and add any fields that aren't already in the
    # ruleset. We need to translate names that end in '_no' to '_id'.
    
    my $field_list = $schema->{_column_list};
    my %has_properties = list_column_properties($table_specifier);
    
    foreach my $column_name ( @$field_list )
    {
	next if $COMMON_FIELD_SPECIAL{$column_name};
	
	my $field_record = $schema->{$column_name};
	my $type = $field_record->{Type};
	
	next if $field_record->{IGNORE};
	
	my $field_name = $column_name;
	
	if ( $field_record->{ALTERNATE_NAME} )
	{
	    $field_name = $field_record->{ALTERNATE_NAME};
	}
	
	elsif ( $field_name =~ /(.*)_no/ )
	{
	    $field_name = $1 . '_id';
	}
	
	next if $ruleset_has_field{$field_name};
	
	my $rr = { optional => $field_name };
	my $doc = "This parameter sets the value of C<$field_name> in the table.";
	
	if ( $type =~ /int\(/ )
	{
	    $doc .= " The value must be an integer.";
	}
	
	if ( $has_properties{$column_name} )
	{
	    my %properties = get_column_properties($table_specifier, $column_name);
	    
	    if ( my $type = $properties{EXTID_TYPE} )
	    {
		$rr->{valid} = VALID_IDENTIFIER($type);
	    }
	}
	
	push @{$ds->{my_param_records}}, $rr;
	
	$ds->validator->add_rules($rs, $rr, $doc);
    }
}


1;
