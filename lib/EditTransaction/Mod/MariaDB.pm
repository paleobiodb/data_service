# 
# EditTransaction::Interface::MariaDB
# 
# This module provides an interface for EditTransaction module to the MariaDB database.
# 
# Author: Michael McClennen

package EditTransaction::Mod::MariaDB;

use strict;

use TableDefs qw(%TABLE get_table_properties get_column_properties set_table_name);

use Encode qw(encode);
use Carp qw(croak);
use Scalar::Util qw(blessed);

our (@CARP_NOT) = qw(EditTransaction);

use feature 'unicode_strings', 'postderef';

use Role::Tiny;

no warnings 'uninitialized';

our $DECIMAL_NUMBER_RE = qr{ ^ \s* ( [+-]? ) \s* (?: ( \d+ ) (?: [.] ( \d* ) )? | [.] ( \d+ ) ) \s*
			     (?: [Ee] \s* ( [+-]? ) \s* ( \d+ ) )? \s* $ }xs;


# The following variables cache retrieved table and column information.
# ---------------------------------------------------------------------

our (%TABLE_INFO_CACHE, %COLUMN_INFO_CACHE, %COLUMN_DIRECTIVE_CACHE);


# Database handles
# ----------------

sub validate_dbh {
    
    my ($edt, $dbh) = @_;
    
    my $message;
    
    if ( $dbh && blessed $dbh && $dbh->can('quote') && $dbh->can('selectrow_array') )
    {
	if ( $dbh->{mariadb_clientinfo} )
	{
	    return 'ok';
	}
	
	else
	{
	    return "'$dbh' is not a MariaDB database handle";
	}
    }
    
    else
    {
	return "'$dbh' is not a valid database handle";
    }
}


# Schemas and properties for tables and columns
# ---------------------------------------------

# table_info_ref ( table_specifier )
# 
# Return a reference to a hash containing cached information about the specified table. If that
# table has not yet been added to the cache, do so. This method is designed to be called only from
# the EditTransaction modules. Client code should use one of the methods from
# EditTransaction::Info instead. If this is called as a class method, a database handle must be
# provided as the second argument.

sub table_info_ref {
    
    my ($edt, $table_specifier, $dbh_arg) = @_;
    
    # First check if %TABLE_INFO_CACHE has an entry for this class and table. If it does not, then
    # fetch the relevant information from the database and modify it according to the defined
    # table and column properties.
    
    my $class = ref $edt || $edt;
    
    unless ( $TABLE_INFO_CACHE{$class}{$table_specifier} )
    {
	fetch_table_schema($edt, $table_specifier, $dbh_arg);
    }
}


# table_column_ref ( table_specifier, [column_name] )
# 
# Return a reference to a hash containing cached information about the columns from the specified
# table. If that table has not yet been added to the cache, do so. If a column name is provided,
# return a reference to that column's record. This method is designed to be called only from the
# EditTransaction modules. Client code should use one of the methods from EditTransaction::Info
# instead.

sub table_column_ref {

    my ($edt, $table_specifier, $column_arg) = @_;
    
    # First check if %TABLE_INFO_CACHE has an entry for this class and table. If it does not, then
    # fetch the relevant information from the database and modify it according to the defined
    # table and column properties.
    
    my $class = ref $edt || $edt;
    
    unless ( $TABLE_INFO_CACHE{$class}{$table_specifier} )
    {
	fetch_table_schema($edt, $table_specifier) || return;
    }
    
    return $column_arg ? $COLUMN_INFO_CACHE{$class}{$table_specifier}{$column_arg} :
	$COLUMN_INFO_CACHE{$class}{$table_specifier};
}


# table_column_list ( table_specifier )
#
# Return a list of the columns in the specified table.

sub table_column_list {
    
    my ($edt, $table_specifier) = @_;
    
    if ( my $tableinfo = $edt->table_info_ref($table_specifier) )
    {
	return $tableinfo->{COLUMN_LIST}->@*;
    }
    
    else
    {
	return;
    }
}


# table_directives_list ( table_specifier )
#
# Return a list of cached column names and directives associated with the specified table. If that
# table has not yet been added to the cache, do so. This method is designed to be called only from
# the EditTransaction modules. Client code should use one of the methods from
# EditTransaction::Info instead.

sub table_directives_list {

    my ($edt, $table_specifier) = @_;
    
    # First check if %TABLE_INFO_CACHE has an entry for this class and table. If it does not, then
    # fetch the relevant information from the database and modify it according to the defined
    # table and column properties.
    
    my $class = ref $edt || $edt;
    
    unless ( defined $TABLE_INFO_CACHE{$class}{$table_specifier} )
    {
	fetch_table_schema($edt, $table_specifier);
    }
    
    return ref $COLUMN_DIRECTIVE_CACHE{$class}{$table_specifier} eq 'HASH' ?
	$COLUMN_DIRECTIVE_CACHE{$class}{$table_specifier}->%* : ();
}


# get_table_handling ( table_specifier, colname )
# 
# Both arguments must be specified. If $colname is '*', return the result of
# table_directives_list. Otherwise, return the table directive (if any) for the
# specified column. If there is none, return '' if the column exists and undef
# otherwise. 

sub get_table_handling {
    
    my ($edt, $table_specifier, $colname) = @_;
    
    croak "you must provide a table specifier and a column name" 
	unless $table_specifier && $colname;
    
    my $class = ref $edt || $edt;
    
    if ( $colname eq '*' )
    {
	return $edt->table_directives_list($table_specifier);;
    }
    
    elsif ( ! defined $TABLE_INFO_CACHE{$class}{$table_specifier} )
    {
	fetch_table_schema($edt, $table_specifier);
    }
    
    if ( $COLUMN_DIRECTIVE_CACHE{$class}{$table_specifier}{$colname} )
    {
	return $COLUMN_DIRECTIVE_CACHE{$class}{$table_specifier}{$colname};
    }
    
    elsif ( $COLUMN_INFO_CACHE{$class}{$table_specifier}{$colname} )
    {
	return '';
    }
    
    else
    {
	return undef;
    }
}


# table_info_present ( table_specifier, [column_name] )
# 
# Return true if the information for the specified table is cached by this module, false
# otherwise. This method will NOT cause the information to be fetched if it is not in the cache.
# 
# If a column name is specified, return true if the specified column is known to
# be part of the table and false otherwise.

sub table_info_present {

    my ($edt, $table_specifier, $column_name) = @_;
    
    my $class = ref $edt || $edt;
    
    if ( @_ == 3 )
    {
	if ( $table_specifier && $column_name )
	{
	    return $COLUMN_INFO_CACHE{$class}{$table_specifier}{$column_name} ? 1 : '';
	}
    }
    
    elsif ( $table_specifier )
    {
	return $TABLE_INFO_CACHE{$class}{$table_specifier} ? 1 : '';
    }
    
    return undef;
}


# fetch_table_schema ( table_specifier, [dbh] )
# 
# Fetch the schema for the specified table, and cache it. The argument can be either a name that
# was defined using the table definition module, or else a name of the form <table> or
# <database>.<table> known to the database. The optional $dbh argument can be provided if this is
# called as a class method. If it is called as an instance method, the EditTransaction instance
# will already have a stored database handle.

sub fetch_table_schema {
    
    my ($edt, $table_specifier, $dbh_arg) = @_;
    
    # If this is called as an instance method, retrieve the necessary information from the
    # EditTransaction instance.
    
    my ($dbh, $debug_mode);
    
    if ( ref $edt )
    {
	$dbh = $edt->dbh || die "missing database handle";
	$debug_mode = $edt->debug_mode;
    }
    
    # If called as a class method, we require a database handle as the second argument. Debug mode
    # is always off in this situation.
    
    elsif ( $dbh_arg && ref($dbh_arg) =~ /DB/ )
    {
	$dbh = $dbh_arg;
    }
    
    else
    {
	croak "you must specify a database handle";
    }
    
    my ($table_key, $table_name);
    
    $DB::single = 1 if ref $edt && $edt->{breakpoint}{schema}{$table_specifier};
    
    # If the specified name is defined in the table definition module, try the name provided
    # by that module. Otherwise, try the specified name directly.
    
    if ( exists $TABLE{$table_specifier} && $TABLE{$table_specifier} )
    {
	$table_key = $table_specifier;
	$table_name = $TABLE{$table_specifier};
    }
    
    else
    {
	$table_name = $table_specifier;
    }
    
    # Confirm that this table exists. The fetch statement is slightly different for a database
    # qualified name than for a simple table name.
    
    my ($sql, $check_table, $quoted_name);
    
    if ( $table_name =~ /(\w+)[.](.+)/ )
    {
	$sql = "SHOW TABLES FROM `$1` LIKE " . $dbh->quote($2);
	$quoted_name = $dbh->quote_identifier($1, $2);
    }
    
    else
    {
	$sql = "SHOW TABLES LIKE " . $dbh->quote($table_name);
	$quoted_name = $dbh->quote_identifier($table_name);
    }
    
    $edt->debug_line("$sql\n") if $debug_mode;
    
    eval {
	($check_table) = $dbh->selectrow_array($sql);
    };
    
    # If the table exists in the database, add it to the table definition system under its own
    # name if it is not already there.
    
    if ( $check_table )
    {
	unless ( $table_key )
	{
	    set_table_name($table_name => $table_name);
	}
    }
    
    # If the table doesn't exist in the database, return undef.
    
    else
    {
	return undef;
	# my $extra = $table_key ? " ($table_key)" : "";
	# croak "The table '$table_name'$extra does not exist in the database";
    }
    
    # If this table was known to the table definition system, fetch the table and column
    # properties.
    
    my (%table_definition, %column_definition, %column_properties);
    
    if ( $table_key )
    {
	%table_definition = get_table_properties($table_key);
	%column_properties = get_column_properties($table_key);
    }
    
    # Add the table name to its definition.
    
    $table_definition{TABLE_NAME} = $table_name;
    $table_definition{QUOTED_NAME} = $quoted_name;
    
    # Then fetch the table column descriptions from the database.
    
    $sql = "SHOW FULL COLUMNS FROM $quoted_name";
    
    $edt->debug_line("$sql\n") if $debug_mode;
    
    my $column_info = $dbh->selectall_arrayref($sql, { Slice => { } });
    
    # Now go through the column description records one by one. Collect a list of column
    # names. Find the primary key if there is one, and add all relevant column properties.
    
    my @column_list;
    my @primary_keys;
    
  COLUMN:
    foreach my $cr ( @$column_info )
    {
	# Each column definition comes to us as a hash. The column name is under 'Field'.
	
	my $colname = $cr->{Field};
	
	$DB::single = 1 if ref $edt && $edt->{breakpoint}{colname}{$colname};
	
	# If any properties have been set for this column using the table definition system, add
	# them now. But if the property IGNORE has a true value, skip this column entirely.
	
	if ( ref $column_properties{$colname} eq 'HASH' )
	{
	    next COLUMN if $column_properties{$colname}{IGNORE};
	    
	    foreach my $p ( keys $column_properties{$colname}->%* )
	    {
		$cr->{$p} = $column_properties{$colname}{$p};
	    }
	}
	
	# Since we are not ignoring this column, add it to the column list.
	
	$column_definition{$colname} = $cr;
	push @column_list, $colname;
	
	# If this is a primary key field, add it to the primary key list.
	
	if ( $cr->{Key} =~ 'PRI' )
	{
	    push @primary_keys, $colname;
	}
	
	# If the column is not nullable and has neither a default value nor the auto_increment
	# property, mark it as NOT_NULL. This will allow us to catch null values and prevent the
	# database from throwing an exception.
	
	if ( $cr->{Null} && $cr->{Null} eq 'NO' &&
	     not ( defined $cr->{Default} ) &&
	     not ( $cr->{Extra} && $cr->{Extra} =~ /auto_increment/i ) )
	{
	    $cr->{NOT_NULL} = 1;
	}
	
	# If the FOREIGN_KEY property was specified and it contains '/', separate it into a table
	# specifier and a column specifier.
	
	if ( my $fk = $cr->{FOREIGN_KEY} )
	{
	    if ( $fk =~ qr{ (.*?) / (.*) }xs )
	    {
		$cr->{FOREIGN_KEY} = $1;
		$cr->{FOREIGN_COL} = $2;
	    }
	}
	
	# If we have information about the column type, unpack it now.
	
	if ( $cr->{Type} )
	{
	    unpack_column_type($cr, $cr->{Type});
	}
    }
    
    # If we found any primary key columns, set the table's PRIMARY_KEY attribute.
    
    if ( @primary_keys == 1 )
    {
	$table_definition{PRIMARY_KEY} = $primary_keys[0];
    }
    
    elsif ( @primary_keys > 2 )
    {
	$table_definition{PRIMARY_KEY} = \@primary_keys;
    }
    
    # Store the list of column names in the table definition.
    
    $table_definition{COLUMN_LIST} = \@column_list;
    
    # If the current class includes a method for post-processing table definitions, call it now.
    
    if ( $edt->can('finish_table_definition') )
    {
	$edt->finish_table_definition(\%table_definition, \%column_definition, \@column_list);
    }
    
    # Then collect up all of the column directives into a single hash. Ignore any 'validate'
    # directives, since that is the default.
    
    my %directives;
    
    foreach my $colname ( @column_list )
    {
	if ( my $directive = $column_definition{$colname}{DIRECTIVE} )
	{
	    $directives{$colname} = $directive unless $directive eq 'validate';
	}
    }
    
    # Cache all the information we have collected, overwriting any previous cache entries.
    
    my $class = ref $edt || $edt;
    
    $TABLE_INFO_CACHE{$class}{$table_specifier} = \%table_definition;
    
    $COLUMN_INFO_CACHE{$class}{$table_specifier} = \%column_definition;
    
    $COLUMN_DIRECTIVE_CACHE{$class}{$table_specifier} = \%directives;
    
    return $TABLE_INFO_CACHE{$class}{$table_specifier};
}


# clear_table_cache ( table_specifier )
# 
# This method is useful primarily for testing purposes. It allows a unit test to
# alter table and column properties and then cause the cached table information
# to be recomputed.

sub clear_table_cache {
    
    my ($edt, $table_specifier) = @_;
    
    my $class = ref $edt || $edt;
    
    if ( $table_specifier eq '*' )
    {
	$TABLE_INFO_CACHE{$class} = undef;
	$COLUMN_INFO_CACHE{$class} = undef;
	$COLUMN_DIRECTIVE_CACHE{$class} = undef;
    }
    
    elsif ( $table_specifier )
    {
	$TABLE_INFO_CACHE{$class}{$table_specifier} = undef;
	$COLUMN_INFO_CACHE{$class}{$table_specifier} = undef;
	$COLUMN_DIRECTIVE_CACHE{$class}{$table_specifier} = undef;
    }
    
    else
    {
	croak "you must provide a table specifier or '*'";
    }
}


# # set_column_properties ( column_record, properties )
# # 
# # Add the specified properties to the specified column record. Those which are listrefs or
# # hashrefs have their immediate contents copied, but no deeper.

# sub set_column_properties {

#     my ($cr, $props) = @_;
    
#     foreach my $name ( keys $props->%* )
#     {
# 	if ( ref $props->{$name} )
# 	{
# 	    $cr->{$name} = copy_property_value($props->{$name});
# 	}
	
# 	else
# 	{
# 	    $cr->{name} = $props->{$name};
# 	}
#     }
# }


# unpack_column_type ( column_record, type_info )
# 
# Unpack the type information string into a list of parameters that can be used by the validation
# routines.

our (%PREFIX_SIZE) = ( tiny => 255,
		       regular => 65535,
		       medium => 16777215,
		       large => 4294967295 );


sub unpack_column_type {

    my ($cr, $typeinfo) = @_;

    if ( $typeinfo =~ qr{ ^ ( var )? ( char | binary ) [(] ( \d+ ) }xs )
    {
	my $type = $2 eq 'char' ? 'text' : 'data';
	my $mode = $1 ? 'variable' : 'fixed';
	my $size = $3;
	my ($charset) = $cr->{Collation} && $cr->{Collation} =~ qr{ ^ ([^_]+) }xs;
	$cr->{TypeMain} = 'char';
	$cr->{TypeParams} = [ $type, $size, $mode, $charset ];
    }
    
    elsif ( $typeinfo =~ qr{ ^ ( tiny | medium | long )? ( text | blob ) (?: [(] ( \d+ ) )? }xs )
    {
	my $type = $2 eq 'text' ? 'text' : 'data';
	my $size = $3 || $PREFIX_SIZE{$1 || 'regular'};
	my ($charset) = $cr->{Collation} && $cr->{Collation} =~ qr{ ^ ([^_]+) }xs;
	$cr->{TypeMain} = 'char';
	$cr->{TypeParams} = [ $type, $size, 'variable', $charset ];
    }
    
    elsif ( $typeinfo =~ qr{ ^ tinyint [(] 1 [)] }xs )
    {
	$cr->{TypeMain} = 'boolean';
    }
    
    elsif ( $typeinfo =~ qr{ ^ (tiny|small|medium|big)? int [(] (\d+) [)] \s* (unsigned)? }xs )
    {
	my $bound = $1 || 'regular';
	my $type = $3 ? 'unsigned' : 'signed';
	$cr->{TypeMain} = $type;
	$cr->{TypeParams} = [ $type, $bound, $2 ];
    }
    
    elsif ( $typeinfo =~ qr{ ^ decimal [(] (\d+) , (\d+) [)] \s* (unsigned)? }xs )
    {
	my $unsigned = $3 ? 'unsigned' : '';
	my $before = $1 - $2;
	my $after = $2;
	
	$after = 10 if $after > 10;	# If people want fields with more than 10 decimals, they should
					# use floating point.

	$cr->{TypeMain} = 'fixed';
	$cr->{TypeParams} = [ $unsigned, $before, $after ];
    }
    
    elsif ( $typeinfo =~ qr{ ^ ( float | double ) (?: [(] ( \d+ ) , ( \d+ ) [)] )? \s* (unsigned)? }xs )
    {
	my $unsigned = $3 ? 'unsigned' : '';
	my $precision = $1;
	my $before = defined $2 ? $2 - $3 : undef;
	my $after = $3;
	$cr->{TypeMain} = 'floating';
	$cr->{TypeParams} = [ $unsigned, $precision, $before, $after ];
    }
    
    elsif ( $typeinfo =~ qr{ ^ bit [(] ( \d+ ) }xs )
    {
	$cr->{TypeMain} = 'bits';
	$cr->{TypeParams} = [ $1 ];
    }
    
    elsif ( $typeinfo =~ qr{ ^ ( enum | set ) [(] (.+) [)] $ }xs )
    {
	my $type = $1;
	my $list = $2;
	my $value_hash = unpack_enum($2);
	$cr->{TypeMain} = 'enum';
	$cr->{TypeParams} = [ $type, $value_hash, $list ];
    }
    
    elsif ( $typeinfo =~ qr{ ^ ( date | time | datetime | timestamp ) \b }xs )
    {
	$cr->{TypeMain} = 'datetime';
	$cr->{TypeParams} = [ $1 ];
    }
    
    elsif ( $typeinfo =~ qr{ ^ ( (?: multi )? (?: point | linestring | polygon ) ) \b }xs )
    {
	$cr->{TypeMain} = 'geometry';
	$cr->{TypeParams} = [ $1 ];
    }
    
    elsif ( $typeinfo =~ qr{ ^ ( geometry (?: collection )? ) \b }xs )
    {
	$cr->{TypeMain} = 'geometry';
	$cr->{TypeParams} = [ $1 ];
    }
    
    else
    {
	$cr->{TypeMain} = 'unknown';
    }
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
	return 'ERROR';
    }

    else
    {
	return $value_hash;
    }
}


# sub copy_property_value {

#     if ( ref $_[0] eq 'HASH' )
#     {
# 	return { $_[0]->%* };
#     }
    
#     elsif ( ref $_[0] eq 'ARRAY' )
#     {
# 	return [ $_[0]->@* ];
#     }
    
#     else
#     {
# 	return $_[0];
#     }
# }


# alter_column_property ( table_specifier, column_name, property, value )
# 
# This routine is intended primarily for testing purposes. It alters a specific
# entry in the %COLUMN_INFO_CACHE. If the property is 'DIRECTIVE', the specified
# value is also stored in %COLUMN_DIRECTIVE_CACHE.

sub alter_column_property {
    
    my ($edt, $table_specifier, $colname, $propname, $value) = @_;
    
    croak "required arguments: table specifier, column name, property name"
	unless $table_specifier && $colname && $propname;
    
    my $class = ref $edt || $edt;
    
    if ( ref $COLUMN_INFO_CACHE{$class}{$table_specifier}{$colname} eq 'HASH' )
    {
	$COLUMN_INFO_CACHE{$class}{$table_specifier}{$colname}{$propname} = $value;
	
	if ( $propname eq 'DIRECTIVE' )
	{
	    $COLUMN_DIRECTIVE_CACHE{$class}{$table_specifier}{$colname} = $value;
	}
	
	return 1;
    }
    
    else
    {
	return undef;
    }
}


# Data column validation routines
# -------------------------------

# The routines in this section all use the same return convention. The result will either be the
# empty list, or it will be some or all of the following:
# 
#     (result, clean_value, additional, clean_no_quote)
# 
# A. If the specified value is valid and no warnings were generated, the empty list is returned.
# 
# B. If specified value is invalid, the first return value will be a listref containing an error
# condition code and parameters. If any additional error or warning was generated, it will appear
# as the third return value. The second value will be undefined and should be ignored.
# 
# C. If a replacement value is generated (i.e. a truncated character string), a 2-4 element list
# will be returned. The third element, if present, will be a warning condition. The fourth
# element, if present, indicates that the returned value is not an SQL literal and should not be
# quoted.
#
# D. If the column value should remain unchanged despite any 'on update' clause, the single value
# 'UNCHANGED' is returned.
# 
# validate_data_column ( cr, value, fieldname )
#
# Check the specified value to make sure it matches the column properties given by $cr. If it does
# not, return an error condition.

sub validate_data_column {

    my ($edt, $cr, $value, $fieldname) = @_;
    
    my ($maintype) = $cr->{TypeMain};
    
    if ( $maintype eq 'char' )
    {
	return $edt->validate_char_value($cr->{TypeParams}, $value, $fieldname, $cr->{ALLOW_TRUNCATE});
    }
    
    elsif ( $maintype eq 'boolean' )
    {
	return $edt->validate_boolean_value($cr->{TypeParams}, $value, $fieldname);
    }
    
    elsif ( $maintype eq 'integer' || $maintype eq 'unsigned' )
    {
	return $edt->validate_integer_value($cr->{TypeParams}, $value, $fieldname);
    }
    
    elsif ( $maintype eq 'fixed' )
    {
	return $edt->validate_fixed_value($cr->{TypeParams}, $value, $fieldname, $cr->{ALLOW_TRUNCATE});
    }
    
    elsif ( $maintype eq 'floating' )
    {
	return $edt->validate_float_value($cr->{TypeParams}, $value, $fieldname);
    }
    
    elsif ( $maintype eq 'enum' )
    {
	return $edt->validate_enum_value($cr->{TypeParams}, $value, $fieldname);
    }
    
    elsif ( $maintype eq 'datetime' )
    {
	return $edt->validate_datetime_value($cr->{TypeParams}, $value, $fieldname);
    }
    
    elsif ( $maintype eq 'geometry' )
    {
	return $edt->validate_geometry_value($cr->{TypeParams}, $value, $fieldname);
    }
    
    # If the data type is anything else, stringify the value and go with it. This
    # might cause problems in occasional situations.
    
    else
    {
	return (1, "$value");
    }
}


# validate_char_value ( type, value, fieldname, can_truncate )
# 
# Check that the specified value is suitable for storing into a boolean column in the
# database. If it is not, return an error condition as a listref.
# 
# If the value is good, return a canonical version suitable for storing into the column. An
# undefined return value will indicate a null. The second return value, if present, will be a
# warning condition as a listref. The third return value, if present, will indicate that the
# returned value has already been quoted.

sub validate_char_value {
    
    my ($edt, $type, $value, $fieldname, $can_truncate) = @_;
    
    my ($subtype, $size, $var, $charset) = ref $type eq 'ARRAY' ? $type->@* : $type || '';
    
    my ($value_size, $truncated, $dbh, $quoted);
    
    # If the character set of a text/char column is not utf8, then encode the value into the
    # proper character set before checking the length.
    
    if ( $subtype eq 'text' && $charset && $charset ne 'utf8' )
    {
	# If the column is latin1, we can do the conversion in Perl.
	
	if ( $charset eq 'latin1' )
	{
	    $value = encode('cp1252', $value);
	    $value_size = length($value);
	}
	
	# Otherwise, we must let the database do the conversion.
	
	else
	{
	    $dbh = $edt->dbh;
	    $quoted = $dbh->quote($value);
	    ($value_size) = $dbh->selectrow_array("SELECT length(convert($quoted using $charset))");
	}
    }
    
    else
    {
	$value_size = length(encode('UTF-8', $value));
    }
    
    # If the size of the value exceeds the size of the column, then we either truncate the data if
    # allowed or else reject the value.
    
    if ( $size && $value_size > $size )
    {
	my $word = $subtype eq 'text' ? 'characters' : 'bytes';
	
	# If we can truncate the value, then do so. If the character set is neither utf8 nor
	# latin1, have the database do it.
	
	if ( $can_truncate )
	{
	    if ( $quoted )
	    {
		($truncated) = $dbh->selectrow_array("SELECT left(convert($quoted using $charset)), $size)");
	    }
	    
	    else
	    {
		$truncated = substr($value, 0, $size);
	    }
	    
	    return (1, $truncated, [ 'W_TRUNC', $fieldname, "value was truncated to $size $word" ]);
	}
	
	else
	{
	    return [ 'E_WIDTH', $fieldname, "value exceeds column size of $size $word (was $value_size)" ];
	}
    }
    
    # If the value is valid as-is, return the empty list.
    
    return;
}


# validate_boolean_value ( type, value, fieldname )
# 
# Check that the specified value is suitable for storing into a boolean column in the
# database. If it is not, add an error condition and return a non-scalar value as a flag to
# indicate that no further processing should be done on it.
# 
# If the value is good, this routine will return a canonical version suitable for storing into the
# column. An undefined return value will indicate a null.

sub validate_boolean_value {
    
    my ($edt, $type, $value, $fieldname) = @_;
    
    # For a boolean column, the value must be either 1 or 0. But we allow 'yes', 'no', 'true',
    # 'false', 'on', 'off' as case-insensitive synonyms. A string that is empty or has only
    # whitespace is turned into a null.
    
    if ( $value =~ qr{ ^ \s* $ }xs )
    {
	return (1, undef);
    }
    
    elsif ( $value =~ qr{ ^ \s* (?: ( 1 | true | yes | on ) | 
				    ( 0 | false | no | off ) ) \s* $ }xsi )
    {
	my $clean_value = $1 ? '1' : '0';
	return (1, $clean_value);
    }
    
    else
    {
	return [ 'E_FORMAT', $fieldname, "value must be one of: 1, 0, true, false, yes, no, on, off" ];
    }
}


# validate_integer_value ( type, value, fieldname )
# 
# Check that the specified value is suitable for storing into an integer column in the
# database. If it is not, add an error condition and return a non-scalar value as a flag to
# indicate that no further processing should be done on it.
# 
# If the value is good, this routine will return a canonical version suitable for storing into the
# column. An undefined return value will indicate a null.

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

sub validate_integer_value {
    
    my ($edt, $type, $value, $fieldname) = @_;
    
    my ($unsigned, $size) = ref $type eq 'ARRAY' ? $type->@* : $type;
    
    my $max = $type eq 'unsigned' ? $UNSIGNED_BOUND{$size} : $SIGNED_BOUND{$size};
    
    # First make sure that the value is either empty or matches the proper format. A value which
    # is empty or contains only whitespace will be treated as a NULL.
    
    if ( $value =~ qr{ ^ \s* $ }xs )
    {
	return (1, undef);
    }
    
    elsif ( $value !~ qr{ ^ \s* ( [-+]? ) \s* ( \d+ ) \s* $ }xs )
    {
	my $phrase = $type eq 'unsigned' ? 'an unsigned' : 'an';
	
	return [ 'E_FORMAT', $fieldname, "value must be $phrase integer" ];
    }
    
    elsif ( $type eq 'unsigned' )
    {
	$value = $2;
	
	if ( $1 && $1 eq '-' )
	{
	    return [ 'E_RANGE', $fieldname, "value must an unsigned integer" ];
	}
	
	elsif ( defined $max && $value > $max )
	{
	    return [ 'E_RANGE', $fieldname, "value must be less than or equal to $max" ];
	}
    }
    
    else
    {
	$value = ($1 && $1 eq '-') ? "-$2" : $2;
	
	if ( defined $max )
	{
	    my $lower = $max + 1;
	    
	    if ( $value > $max || (-1 * $value) > $lower )
	    {
		return [ 'E_RANGE', $fieldname, "value must lie between -$lower and $max" ];
	    }
	}
    }
    
    # If the value is valid as-is, return the empty list.

    return;
}


# validate_fixed_value ( type, value, fieldname )
# 
# Check that the specified value is suitable for storing into a fixed-point decimal column in the
# database. If it is not, add an error condition and return a non-scalar value as a flag to
# indicate that no further processing should be done on it.
# 
# If the value is good, this routine will return a canonical version suitable for storing into the
# column. An undefined return value will indicate a null.

sub validate_fixed_value {

    my ($edt, $type, $value, $fieldname, $can_truncate) = @_;
    
    my ($unsigned, $whole, $precision) = ref $type eq 'ARRAY' ? $type->@* : $type;
    
    my $additional;
    
    # First make sure that the value is either empty or matches the proper format.  A value which
    # is empty or contains only whitespace is turned into NULL.
    
    if ( $value =~ qr{ ^ \s* $ }xs )
    {
	return (1, undef);
    }
    
    elsif ( $value !~ $DECIMAL_NUMBER_RE )
    {
	my $phrase = $unsigned ? 'an unsigned' : 'a';
	
	return ['E_FORMAT', $fieldname, "value must be $phrase decimal number" ];
    }
    
    else
    {
	# If the column is unsigned, make sure there is no minus sign.
	
	if ( $unsigned && defined $1 && $1 eq '-' )
	{
	    return [ 'E_RANGE', $fieldname, "value must be an unsigned decimal number" ];
	}
	
	# Now put the number back together from the regex captures. If there is an
	# exponent, reformat it as a fixed point.
	
	my $sign = $1 && $1 eq '-' ? '-' : '';
	my $intpart = $2 // '';
	my $fracpart = $3 // $4 // '';
	
	if ( $6 )
	{
	    my $exponent = ($5 && $5 eq '-' ? "-$6" : $6);
	    my $formatted = sprintf("%.10f", "${intpart}.${fracpart}E${exponent}");
	    
	    ($intpart, $fracpart) = split(/[.]/, $formatted);
	}
	
	# Check that the number of digits is not exceeded, either before or after the decimal. In
	# the latter case, we add an error unless the column property ALLOW_TRUNCATE is set in
	# which case we add a warning.
	
	$intpart =~ s/^0+//;
	$fracpart =~ s/0+$//;
	
	if ( $intpart && length($intpart) > $whole )
	{
	    my $total = $whole + $precision;
	    
	    return [ 'E_RANGE', $fieldname, "value is too large for decimal($total,$precision)" ];
	}
	
	# Rebuild the value, with the fracional part trimmed.
	
	my $clean_value = $sign;
	$clean_value .= $intpart || '0';
	$clean_value .= '.' . substr($fracpart, 0, $precision);
	
	# If the value is too wide, return either an error condition or the truncated value and a warning.
	
	if ( $fracpart && length($fracpart) > $precision )
	{
	    my $total = $whole + $precision;
	    
	    if ( $can_truncate )
	    {
		return (1, $clean_value, [ 'W_TRUNC', $fieldname,
					   "value has been truncated to decimal($total,$precision)" ]);
	    }
	    
	    else
	    {
		return [ 'E_WIDTH', $fieldname,
			 "too many decimal digits for decimal($total,$precision)" ];
	    }
	}
	
	# If the clean value is different from the original but is equivalent, return it.

	elsif ( $clean_value ne $value )
	{
	    return (1, $clean_value);
	}
    }

    # If the value is valid as-is, return the empty list.

    return;
}


# validate_float_value ( type, value, fieldname )
# 
# Check that the specified value is suitable for storing into a floating-point decimal column in the
# database. If it is not, add an error condition and return a non-scalar value as a flag to
# indicate that no further processing should be done on it.
# 
# If the value is good, this routine will return a canonical version suitable for storing into the
# column. An undefined return value will indicate a null.

sub validate_float_value {

    my ($edt, $type, $value, $fieldname) = @_;
    
    my ($unsigned, $precision) = ref $type eq 'ARRAY' ? $type->@* : $type;
    
    # First make sure that the value is either empty or matches the proper format. A value which
    # is empty or contains only whitespace will be treated as a NULL.
    
    if ( $value =~ qr{ ^ \s* $ }xs )
    {
	return undef;
    }
    
    elsif ( $value !~ $DECIMAL_NUMBER_RE )
    {
	my $phrase = $unsigned ? 'an unsigned' : 'a';
	
	return [ 'E_FORMAT', $fieldname, "value must be $phrase floating point number" ];
    }
    
    else
    {
	my $sign = (defined $1 && $1 eq '-') ? '-' : '';
	
	# If the column is unsigned, make sure there is no minus sign.
	
	if ( $unsigned && $sign eq '-' )
	{
	    return [ 'E_RANGE', $fieldname, "value must be an unsigned floating point number" ];
	}
	
	# Put the pieces of the value back together.
	
	my $clean_value = $sign . ( $2 // '' ) . '.';
	$clean_value .= ( $3 // $4 // '' );
	
	if ( $6 )
	{
	    my $exp_sign = $5 eq '-' ? '-' : '';
	    $clean_value .= 'E' . $exp_sign . $6;
	}
	
	# Then check that the number is not too large to be represented, given the size of the
	# field. We are conservative in the bounds we check. We do not check for the number of
	# decimal places being exceeded, because floating point is naturally inexact. Also, if
	# maximum digits were specified we ignore these.
			    
	my $bound = $precision eq 'double' ? 1E308 : 1E38;
	my $word = $precision eq 'float' ? 'single' : 'double';
	
	if ( $value > $bound || ( $value < 0 && -$value > $bound ) )
	{
	    return [ 'E_RANGE', $fieldname, "magnitude is too large for $word-precision floating point" ];
	}

	# If the clean value is different from the original, return that.

	elsif ( $clean_value ne $value )
	{
	    return (1, $clean_value);
	}
    }

    # If the value is valid as-is, return the empty list.

    return;
}


# validate_enum_value ( type, value, fieldname )
# 
# Check that the specified value is suitable for storing into an enumerated or set valued column in the
# database. If it is not, add an error condition and return a non-scalar value as a flag to
# indicate that no further processing should be done on it.
# 
# If the value is good, this routine will return a canonical version suitable for storing into the
# column. An undefined return value will indicate a null.

sub validate_enum_value {

    my ($edt, $type, $value, $fieldname) = @_;
    
    my ($subtype, $good_values) = ref $type eq 'ARRAY' ? $type->@* : $type;
    
    # If the data type is either 'set' or 'enum', then we check to make sure that the value is one
    # of the allowable ones. We always match without regard to case, using the Unicode 'fold case'
    # function (fc).
    
    use feature 'fc';
    
    $value =~ s/^\s+//;
    $value =~ s/\s+$//;
    
    my @raw = $value;
    
    # if ( $type eq 'set' )
    # {
    # 	my $sep = $column_defn->{VALUE_SEPARATOR} || qr{ \s* , \s* }xs;
    # 	@raw = split $sep, $value;
    # }
    
    my (@good, @bad);
    
    foreach my $v ( @raw )
    {
	next unless defined $v && $v ne '';

	if ( ! $good_values )
	{
	    push @good, $v;
	}
	
	elsif ( ref $good_values && $good_values->{fc $v} )
	{
	    push @good, $v;
	}
	
	else
	{
	    push @bad, $v;
	}
    }
    
    if ( @bad )
    {
	my $value_string = join(', ', @bad);
	my $word = @bad > 1 ? 'values' : 'value';
	my $word2 = @bad > 1 ? 'are' : 'is';
	
	return [ 'E_RANGE', $fieldname, "$word '$value_string' $word2 not allowed for this table column" ];
    }
    
    elsif ( @good )
    {
	return (1, join(',', @good));
    }
    
    else
    {
	return (1, undef);
    }
}


# validate_datetime_value ( type, value, fieldname )
# 
# Check that the specified value is suitable for storing into a time or date or datetime valued
# column in the database. If it is not, add an error condition and return a non-scalar value as a
# flag to indicate that no further processing should be done on it.
# 
# If the value is good, this routine will return a canonical version suitable for storing into the
# column. An undefined return value will indicate a null.

sub validate_datetime_value {
    
    my ($edt, $type, $value, $fieldname) = @_;
    
    my ($specific) = ref $type eq 'ARRAY' ? $type->@* : $type;
    
    if ( $value =~ qr{ ^ now (?: [(] [)] ) ? $ }xsi )
    {
	return (1, "NOW()", undef, 1);
    }
    
    elsif ( $value =~ qr{ ^ \d\d\d\d\d\d\d\d\d\d+ $ }xs )
    {
	return (1, "FROM_UNIXTIME($value)", undef, 1);
    }
    
    elsif ( $specific eq 'time' )
    {
	if ( $value =~ qr{ ^ \d\d : \d\d : \d\d $ }xs )
	{
	    return;
	}

	else
	{
	    return [ 'E_FORMAT', $fieldname, "invalid time value '$value'" ];
	}
    }
    
    elsif ( $value =~ qr{ ^ ( \d\d\d\d - \d\d - \d\d ) ( \s+ \d\d : \d\d : \d\d ) ? $ }xs )
    {
	if ( $2 || $specific eq 'date' )
	{
	    return;
	}
	
	else
	{
	    return (1, "$value 00:00:00");
	}
    }
    
    else
    {
	return [ 'E_FORMAT', $fieldname, "invalid datetime value '$value'" ];
    }
}


# validate_geometry_value ( type, value, fieldname )
# 
# Check that the specified value is suitable for storing into a geometry valued column in the
# database. If it is not, add an error condition and return a non-scalar value as a flag to
# indicate that no further processing should be done on it.
# 
# If the value is good, this routine will return a canonical version suitable for storing into the
# column. An undefined return value will indicate a null.

sub validate_geometry_value {
    
    my ($edt, $type, $value, $fieldname) = @_;
    
    my ($specific) = ref $type eq 'ARRAY' ? $type->@* : $type || 'unknown';
    
    # $$$ we still need to write some code to validate these.
    
    return;
}


# SQL expressions for use elsewhere in the codebase
# -------------------------------------------------

sub sql_current_timestamp {
    
    return 'NOW()';
}


# Database query routines
# -----------------------

# count_matching_rows ( table_specifier, expression )
#
# Return a count of rows from the specified table that match the specified SQL expression.

sub count_matching_rows {
    
    my ($edt, $table_specifier, $expression) = @_;
    
    my $sql = "SELECT count(*) FROM $TABLE{$table_specifier} WHERE $expression";
    
    $edt->debug_line("$sql\n") if $edt->debug_mode;
    
    my ($count) = $edt->dbh->selectrow_array($sql);
    
    return $count;
}


# Action execution routines
# -------------------------

# execute_insert ( action )
#
# Execute an 'insert' action on the database.

sub execute_insert {

    my ($edt, $action) = @_;
    
    # Check to make sure that we have non-empty column/value lists, and that the number of columns
    # and values is equal and non-zero.
    
    my $table = $action->table;    
    my $cols = $action->column_list;
    my $vals = $action->value_list;
    
    unless ( ref $cols eq 'ARRAY' && ref $vals eq 'ARRAY' && @$cols && @$cols == @$vals )
    {
	$edt->add_condition($action, 'E_EXECUTE', 'column/value mismatch or missing on insert');
	return;
    }
    
    # Construct the INSERT statement.
    
    my $dbh = $edt->dbh;
    
    my $column_string = join(',', @$cols);
    my $value_string = join(',', @$vals);
    
    my $sql = "	INSERT INTO $TABLE{$table} ($column_string)
		VALUES ($value_string)";
    
    # If the following flag is set, deliberately generate an SQL error for testing purposes.
    
    if ( $EditTransaction::TEST_PROBLEM{sql_error} )
    {
	$sql .= " XXXX";
    }
    
    $edt->debug_line("$sql\n") if $edt->debug_mode;
    
    my ($new_keyval);
    
    # Execute the statement inside an eval block, to catch any exceptions that might be thrown.
    
    eval {
	
	$edt->before_action($action, 'insert', $table);
	
	# Execute the insert statement itself, provided there were no errors and the action
	# was not aborted during the execution of before_action.
	
	if ( $action->can_proceed )
	{
	    my $result = $dbh->do($sql);
	    
	    $action->set_status('executed');
	    
	    # If the insert succeeded, get and store the new primary key value. Otherwise, add an
	    # error condition. Unlike update, replace, and delete, if an insert statement fails
	    # that counts as a failure of the action.
	    
	    if ( $result )
	    {
		$new_keyval = $dbh->last_insert_id(undef, undef, undef, undef);
		$action->set_keyval($new_keyval);
		$action->set_result($new_keyval);
	    }
	    
	    unless ( $new_keyval )
	    {
		$edt->add_condition($action, 'E_EXECUTE', 'insert statement failed');
	    }
	    
	    # Finally, call the 'after_action' method.
	    
	    $edt->after_action($action, 'insert', $table, $new_keyval);
	}
    };
    
    # If an exception occurred, print it to the error stream and add a corresponding error
    # condition. Any exeption that occurs after the database statement is executed counts as a
    # fatal error for the transaction, because there is no way to go back and undo that statement
    # if PROCEED is in force.
    
    if ( $@ )
    {
	$edt->error_line($@);
	
	if ( $@ =~ /duplicate entry '(.*)' for key '(.*)' at/i )
	{
	    my $value = $1;
	    my $key = $2;
	    $edt->add_condition($action, 'E_DUPLICATE', $value, $key);
	}
	
	else
	{
	    $action->pin_errors if $action->status eq 'executed';
	    $edt->add_condition($action, 'E_EXECUTE', 'an exception occurred during execution');
	}
    };
    
    # If the SQL statement succeeded, increment the executed count.
    
    if ( $action->has_executed && $new_keyval )
    {
	$edt->{exec_count}++;
    }
    
    # Otherwise, set the action status to 'failed' and increment the fail count unless the action
    # was aborted before execution.
    
    elsif ( $action->status ne 'aborted' )
    {
	$action->set_status('failed');
	$edt->{fail_count}++;
    }
    
    return;
}


# execute_replace ( action )
#
# Execute an 'replace' action on the database.

sub execute_replace {

    my ($edt, $action) = @_;
    
    # Check to make sure that we actually have column/value lists, and that the number of columns
    # and values is equal and non-zero.
    
    my $table = $action->table;
    my $cols = $action->column_list;
    my $vals = $action->value_list;
    
    unless ( ref $cols eq 'ARRAY' && ref $vals eq 'ARRAY' && @$cols && @$cols == @$vals )
    {
	$edt->add_condition($action, 'E_EXECUTE', 'column/value mismatch or missing on replace');
	return;
    }
    
    # Construct the REPLACE statement.
    
    my $dbh = $edt->dbh;
    
    my $column_list = join(',', @$cols);
    my $value_list = join(',', @$vals);
    
    my $sql = "	REPLACE INTO $TABLE{$table} ($column_list)
		VALUES ($value_list)";
    
    # If the following flag is set, deliberately generate an SQL error for testing purposes.
    
    if ( $EditTransaction::TEST_PROBLEM{sql_error} )
    {
	$sql .= " XXXX";
    }
    
    $edt->debug_line("$sql\n") if $edt->debug_mode;
    
    # Execute the statement inside a try block, to catch any exceptions that might be thrown.
    
    eval {
	
	# If we are logging this action, then fetch the existing record if any.
	
	# unless ( $edt->allows('NO_LOG_MODE') || get_table_property($table, 'NO_LOG') )
	# {
	#     $edt->fetch_old_record($action, $table);
	# }
	
	# Start by calling the 'before_action' method.
	
	$edt->before_action($action, 'replace', $table);
	
	# Then execute the replace statement itself, provided there are no errors and the action
	# was not aborted. If the replace statement returns a zero result and does not throw an
	# exception, that means that the new record was identical to the old one. This is counted
	# as a successful execution, and is marked with a warning.
	
	if ( $action->can_proceed )
	{
	    my $result = $dbh->do($sql);
	    
	    $action->set_status('executed');
	    $action->set_result($result);
	    
	    unless ( $result )
	    {
		$edt->add_condition($action, 'W_UNCHANGED', 'New record is identical to the old');
	    }
	    
	    $edt->after_action($action, 'replace', $table, $result);
	}
    };
    
    # If an exception occurred, print it to the error stream and add a corresponding error
    # condition. Any exeption that occurs after the database statement is executed is
    # automatically a fatal error for the transaction, because there is no way to go back and undo
    # that statement if PROCEED is in force.
    
    if ( $@ )
    {	
	$edt->error_line($@);
	
	if ( $@ =~ /duplicate entry '(.*)' for key '(.*)' at/i )
	{
	    my $value = $1;
	    my $key = $2;
	    $edt->add_condition($action, 'E_DUPLICATE', $value, $key);
	}
	
	else
	{
	    $action->pin_errors if $action->status eq 'executed';
	    $edt->add_condition($action, 'E_EXECUTE', 'an exception occurred during execution');
	}
    }
    
    # If the SQL statement succeeded, increment the executed count.
    
    if ( $action->has_executed )
    {
	$edt->{exec_count}++;
    }
    
    # Otherwise, set the action status to 'failed' unless the action was aborted before execution.
    
    elsif ( $action->status ne 'aborted' )
    {
	$action->set_status('failed');
	$edt->{fail_count}++;
    }
    
    return;
}


# execute_update ( action )
#
# Execute an 'update' action on the database.

sub execute_update {

    my ($edt, $action) = @_;
    
    # Check to make sure that we actually have column/value lists, and that the number of columns
    # and values is equal and non-zero.
    
    my $table = $action->table;
    my $cols = $action->column_list;
    my $vals = $action->value_list;
    
    unless ( ref $cols eq 'ARRAY' && ref $vals eq 'ARRAY' && @$cols && @$cols == @$vals )
    {
	$edt->add_condition($action, 'E_EXECUTE', 'column/value mismatch or missing on update');
	return;
    }
    
    # Construct the UPDATE statement.
    
    my $dbh = $edt->dbh;
    my $set_list = '';
    
    foreach my $i ( 0..$#$cols )
    {
	$set_list .= ', ' if $set_list;
	$set_list .= "$cols->[$i]=$vals->[$i]";
    }
    
    my $keyexpr = $action->keyexpr;
    
    my $sql = "	UPDATE $TABLE{$table} SET $set_list
		WHERE $keyexpr";
    
    # If the following flag is set, deliberately generate an SQL error for testing purposes.
    
    if ( $EditTransaction::TEST_PROBLEM{sql_error} )
    {
	$sql .= " XXXX";
    }
    
    $edt->debug_line("$sql\n") if $edt->debug_mode;
    
    # Execute the statement inside a try block. If it fails, add either an error or a warning
    # depending on whether this EditTransaction allows PROCEED.
    
    eval {
	
	# If we are logging this action, then fetch the existing record.
	
	# unless ( $edt->allows('NO_LOG_MODE') || get_table_property($table, 'NO_LOG') )
	# {
	#     $edt->fetch_old_record($action, $table, $keyexpr);
	# }
	
	# Start by calling the 'before_action' method.
	
	$edt->before_action($action, 'update', $table);
	
	# Then execute the update statement itself, provided there are no errors and the action
	# has not been aborted. If the update statement returns a result less than the number of
	# matching records, that means at least one updated record was identical to the old
	# one. This is counted as a successful execution, and is marked with a warning.
	
	if ( $action->can_proceed )
	{
	    my $result = $dbh->do($sql);
	    
	    $action->set_status('executed');
	    $action->set_result($result);
	    
	    if ( $action->keymult && $result < $action->keyvalues )
	    {
		$sql = "SELECT count(*) FROM $TABLE{$table} WHERE $keyexpr";
		
		$edt->debug_line("$sql\n") if $edt->debug_mode;
		
		my ($found) = $dbh->selectrow_array($sql);
		
		my $missing = $action->keyvalues - $found;

		if ( $missing > 0 )
		{
		    $edt->add_condition($action, 'W_NOT_FOUND',
					"$missing key value(s) were not found");
		}
		
		my $unchanged = $found - $result;
		
		if ( $unchanged )
		{
		    $edt->add_condition($action, 'W_UNCHANGED',
					"$unchanged record(s) were unchanged by the update");
		}
	    }
	    
	    elsif ( ! $result )
	    {
		$edt->add_condition($action, 'W_UNCHANGED', 'Record was unchanged by the update');
	    }
	    
	    $edt->after_action($action, 'replace', $table, $result);
	}
    };
    
    if ( $@ )
    {
	$edt->error_line($@);
	
	if ( $@ =~ /duplicate entry '(.*)' for key '(.*)' at/i )
	{
	    my $value = $1;
	    my $key = $2;
	    $edt->add_condition($action, 'E_DUPLICATE', $value, $key);
	}
	
	else
	{
	    $action->pin_errors if $action->status eq 'executed';
	    $edt->add_condition($action, 'E_EXECUTE', 'an exception occurred during execution');
	}
    };
    
    # If the SQL statement succeeded, increment the executed count.
    
    if ( $action->has_executed )
    {
	$edt->{exec_count}++;
    }
    
    # Otherwise, set the action status to 'failed' unless the action was aborted before execution.
    
    elsif ( $action->status ne 'aborted' )
    {
	$action->set_status('failed');
	$edt->{fail_count}++;
    }
    
    return;
}


# execute_update ( action )
#
# Execute an 'update' action on the database.

sub execute_delete {

    my ($edt, $action) = @_;
    
    # Construct the DELETE statement.
    
    my $dbh = $edt->dbh;
    my $table = $action->table;
    my $keyexpr = $action->keyexpr;
    
    my $sql = "	DELETE FROM $TABLE{$table} WHERE $keyexpr";
    
    # If the following flag is set, deliberately generate an SQL error for
    # testing purposes.
    
    if ( $EditTransaction::TEST_PROBLEM{sql_error} )
    {
	$keyexpr .= ' XXXX';
    }
    
    $edt->debug_line( "$sql\n" ) if $edt->debug_mode;
    
    # Execute the statement inside a try block. If it fails, add either an error or a warning
    # depending on whether this EditTransaction allows PROCEED.
    
    eval {
	
	# If we are logging this action, then fetch the existing record.
	
	# unless ( $edt->allows('NO_LOG_MODE') || get_table_property($table, 'NO_LOG') )
	# {
	#     $edt->fetch_old_record($action, $table, $keyexpr);
	# }
	
	# Start by calling the 'before_action' method. This is designed to be overridden by
	# subclasses, and can be used to do any necessary auxiliary actions to the database. The
	# default method does nothing.    
	
	$edt->before_action($action, 'delete', $table);
	
	# Then execute the delete statement itself, provided the action has not been aborted.
	
	if ( $action->can_proceed )
	{
	    my $result = $dbh->do($sql);
	    
	    $action->set_status('executed');
	    $action->set_result($result);
	    
	    if ( $action->keymult && $result < $action->keyvalues )
	    {
		my $missing = $action->keyvalues - $result;
		
		$edt->add_condition($action, 'W_NOT_FOUND', "$missing key values(s) were not found");
	    }
	    
	    $edt->after_action($action, 'delete', $table, $result);
	}
    };
    
    if ( $@ )
    {	
	$edt->error_line($@);
	$action->pin_errors if $action->status eq 'executed';
	$edt->add_condition($action, 'E_EXECUTE', 'an exception occurred during execution');
    };
    
    # If the SQL statement succeeded, increment the executed count.
    
    if ( $action->has_executed )
    {
	$edt->{exec_count}++;
    }
    
    # Otherwise, set the action status to 'failed' unless the action was aborted before execution.
    
    elsif ( $action->status ne 'aborted' )
    {
	$action->set_status('failed');
	$edt->{fail_count}++;
    }
    
    return;
}


# fetch_record ( table_arg, keyexpr )
#
# Return the record from the specified table with the specified key expression. If the table does
# not exist or the key expression does not select any records, return undefined.

sub fetch_record {

    my ($edt, $table_specifier, $keyexpr) = @_;
    
    if ( my $tableinfo = $edt->table_info_ref($table_specifier) )
    {
	my $quoted_name = $tableinfo->{QUOTED_NAME};
	my $sql = "SELECT * FROM $quoted_name WHERE $keyexpr LIMIT 1";
	
	$edt->debug_line("$sql\n") if $edt->debug_mode;
	
	return $edt->dbh->selectrow_hashref($sql);
    }

    else
    {
	return;
    }
}    


# get_old_values ( action, table, fields )
# 
# Fetch the specified columns from the table record whose key is given by the specified action.

sub get_old_values {

    my ($edt, $tablename, $keyexpr, $fields) = @_;

    return unless $keyexpr;
    
    if ( my $tableinfo = $edt->table_info_ref($tablename) )
    {
	my $quoted_name = $tableinfo->{QUOTED_NAME};
	
	my $sql = "SELECT $fields FROM $quoted_name WHERE $keyexpr LIMIT 1";
	
	$edt->debug_line("$sql\n") if $edt->debug_mode;
	
	return $edt->dbh->selectrow_array($sql);
    }

    else
    {
	return;
    }
}


1;
