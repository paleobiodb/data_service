# 
# EditTransaction::Interface::MariaDB
# 
# This module provides an interface for EditTransaction module to the MariaDB database.
# 
# Author: Michael McClennen

package EditTransaction::DBF::MariaDB;

use strict;

use EditTransaction qw($REGISTERED_APP);

use TableDefs qw(%TABLE get_table_defn get_table_column_defn set_table_name list_column_property_names);

use Encode qw(encode);
use Carp qw(croak);

our (@CARP_NOT) = qw(EditTransaction);

use feature 'unicode_strings', 'postderef';


our $DECIMAL_NUMBER_RE = qr{ ^ \s* ( [+-]? ) \s* (?: ( \d+ ) (?: [.] ( \d* ) )? | [.] ( \d+ ) ) \s*
			     (?: [Ee] \s* ( [+-]? ) \s* ( \d+ ) )? \s* $ }xs;


# The following variables cache retrieved table and column information.
# ---------------------------------------------------------------------

our (%TABLE_SCHEMA_CACHE, %COLUMN_SCHEMA_CACHE, %COLUMN_DIRECTIVE_CACHE);


# After this module is compiled, register it with EditTransaction.pm.
# -------------------------------------------------------------------

UNITCHECK {
    EditTransaction->register_dbf('MariaDB');
    
    EditTransaction->register_hook('get_table_description', \&get_table_description);
    EditTransaction->register_hook('get_column_description', \&get_column_description);
    EditTransaction->register_hook('fetch_table_schema', \&fetch_table_schema);
    EditTransaction->register_hook('get_column_property', \&get_column_property);
    EditTransaction->register_hook('alter_column_property', \&alter_column_property);
    
    EditTransaction->register_hook_value('validate_data_column', 'text', \&validate_char_value);
    EditTransaction->register_hook_value('validate_data_column', 'data', \&validate_char_value);
    EditTransaction->register_hook_value('validate_data_column', 'boolean', \&validate_boolean_value);
    EditTransaction->register_hook_value('validate_data_column', 'integer', \&validate_integer_value);
    EditTransaction->register_hook_value('validate_data_column', 'fixed', \&validate_fixed_value);
    EditTransaction->register_hook_value('validate_data_column', 'floating', \&validate_float_value);
    EditTransaction->register_hook_value('validate_data_column', 'enum', \&validate_enum_value);
    EditTransaction->register_hook_value('validate_data_column', 'set', \&validate_enum_value);
    EditTransaction->register_hook_value('validate_data_column', 'geometry', \&validate_geometry_value);
    EditTransaction->register_hook_value('validate_data_column', 'datetime', \&validate_datetime_value);
    
    EditTransaction->register_hook('execute_insert', \&execute_insert);
    EditTransaction->register_hook('execute_replace', \&execute_replace);
    EditTransaction->register_hook('execute_update', \&execute_update);
    EditTransaction->register_hook('execute_delete', \&execute_delete);
};


# Schemas and properties for tables and columns
# ---------------------------------------------

# get_table_description ( table_specifier )
#
# Fetch and cache the schema of the specified table, if it does not already appear in the
# cache. Return a list of two references. The first provides the table properties, and the second
# is a hash with one key per column. The values are secondary hashes that provide the column
# properties.

sub get_table_description {
    
    my ($edt, $table_arg) = @_;
    
    my $app = $edt->app;
    
    unless ( defined $COLUMN_SCHEMA_CACHE{$app}{$table_arg} )
    {
	&fetch_table_schema($edt, $table_arg);
    }
    
    return ($TABLE_SCHEMA_CACHE{$app}{$table_arg}, $COLUMN_SCHEMA_CACHE{$app}{$table_arg});
}


# get_column_description ( table_specifier, column_name )
#
# If a column name is specified, return a reference to the requested column record. Otherwise,
# return a list of all column records for the specified table. If this information does not appear
# in our cache, then fetch it and cache it first.

sub get_column_description {

    my ($edt, $table_arg, $column_arg) = @_;
    
    my $app = $edt->app;
    
    unless ( defined $COLUMN_SCHEMA_CACHE{$app}{$table_arg} )
    {
	&fetch_table_schema($edt, $table_arg);
    }

    if ( defined $column_arg )
    {
	return $COLUMN_SCHEMA_CACHE{$app}{$table_arg}{$column_arg};
    }

    else
    {
	return $COLUMN_SCHEMA_CACHE{$app}{$table_arg};
    }
}


# table_schema_present ( table_specifier, column_name )
# 
# Return true if the specified table schema has been fetched and cached. If the second argument is
# non-empty, return true if the specified column name exists within it.

sub table_schema_present {
    
    my ($edt, $table_specifier, $column_name) = @_;
    
    croak "required argument: table specifier" unless $table_specifier;
    
    my $app = $edt->app;
    
    if ( $column_name )
    {
	return defined $COLUMN_SCHEMA_CACHE{$app}{$table_specifier}{$column_name};
    }
    
    else
    {
	return defined $COLUMN_SCHEMA_CACHE{$app}{$table_specifier};
    }
}


# fetch_table_schema ( table_specifier )
# 
# Fetch the schema for the specified table, and cache it. The argument can be either a name that
# was defined using the table definition module, or else a name of the form <table> or
# <database>.<table> known to the database.

sub fetch_table_schema {
    
    my ($edt, $table_arg) = @_;
    
    my $app = $edt->app;
    
    my $dbh = $edt->dbh || die "missing database handle";
    
    my $debug_mode = $edt->debug_mode;
    
    my ($table_key, $table_name);
    
    # If the specified name is defined in the table definition module, try the name provided
    # by that module. Otherwise, try the specified name directly.
    
    if ( defined $TABLE{$table_arg} && $TABLE{$table_arg} ne '' )
    {
	$table_key = $table_arg;
	$table_name = $TABLE{$table_arg};
    }
    
    # Otherwise, check to see if the given table name exists in the database. If so, add a table
    # definition under its own name.
    
    else
    {
	$table_key = $table_arg;
	$table_name = $table_arg;
    }
    
    # Confirm that this table exists. The fetch statement is slightly different for a database
    # qualified name than for a simple table name.
    
    my ($sql, $check_table, $quoted_table);
    
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
    
    $edt->debug_line("$sql\n") if $debug_mode;
    
    eval {
	($check_table) = $dbh->selectrow_array($sql);
    };
    
    # If the table doesn't exist in the database, throw an exception.
    
    if ( not $check_table )
    {
	my $extra = $table_key ? " ($table_key)" : "";
	croak $table_key "The table '$table_name'$extra does not exist in the database";
    }
    
    # Otherwise, fetch all of the available information about this table.
    
    my (%table_definition, %column_definition, %column_properties);
    
    # If properties have been defined for it, fetch them now from the table definition system.
    
    if ( $table_specifier )
    {
	%table_definition = get_table_defn($table_specifier);
	%column_properties = get_table_column_defn($table_specifier);
    }
    
    # Then fetch the table column descriptions from the database.
    
    $sql = "SHOW FULL COLUMNS FROM $quoted_table";
    
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

	# If the column property IGNORE has a true value, ignore this column.
	
	next COLUMN if $column_properties{$colname}{IGNORE};
	
	# Otherwise, add it to the column list.
	
	$column_definition{$colname} = $cr;
	push @column_list, $colname;
	
	# If this is a primary key field, add it to the primary key list.
	
	if ( $cr->{Key} =~ 'PRI' )
	{
	    push @primary_keys, $colname;
	}
	
	# If the column is not nullable and has neither a default value nor an auto_increment,
	# mark it as NOT_NULL. Otherwise, a database error will be generated when we try to insert
	# or update a record with a null value for this column.
	
	if ( $cr->{Null} && $cr->{Null} eq 'NO' &&
	     not ( defined $cr->{Default} ) &&
	     not ( $cr->{Extra} && $cr->{Extra} =~ /auto_increment/i ) )
	{
	    $cr->{NOT_NULL} = 1;
	}
	
	# If any properties have been set for this column using the table definition system, add
	# them now.
	
	if ( $column_properties{$colname} && $column_properties{$colname}->%* )
	{
	    set_column_properties($cr, $column_properties{$colname});
	}
	
	# If the FOREIGN_KEY property was specified, and it contains a /, separate it into a table
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

	# The two directives handled by default are 'ts_created' and 'ts_modified'. If this column
	# has one of them, add an INSERT_FILL or UPDATE_FILL property.
	
	if ( $cr->{DIRECTIVE} && $cr->{DIRECTIVE} =~ /^ts_created$|^ts_modified$/ )
	{
	    unless ( $cr->{Default} =~ /current_timestamp/i )
	    {
		$cr->{INSERT_FILL} = 'NOW()';
	    }

	    unless ( $cr->{Extra} =~ /on update/i )
	    {
		$cr->{UPDATE_FILL} = 'NOW()';
	    }
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
    
    # If the current application module includes a hook for post-processing table definitions,
    # call it now.
    
    $edt->call_hook('finish_table_definition', \%table_definition, \%column_definition, \@column_list);
    
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
    
    $TABLE_SCHEMA_CACHE{$app}{$table_specifier} = \%table_definition;
    
    $COLUMN_SCHEMA_CACHE{$app}{table_specifier} = \%column_definition;
    
    $COLUMN_DIRECTIVE_CACHE{$app}{$table_specifier} = \%directives;
    
    return 1;
}


# set_column_properties ( column_record, properties )
# 
# Add the specified properties to the specified column record. Those which are listrefs or
# hashrefs have their immediate contents copied, but no deeper.

sub set_column_properties {

    my ($cr, $props) = @_;
    
    foreach my $name ( keys $props->%* )
    {
	cr->{$name} = copy_property_value($props->{$name});
    }
}


# unpack_column_type ( column_record, type_info )
# 
# Unpack the type information string into a list of parameters that can be used by the validation
# routines.

our (%PREFIX_SIZE) = ( tiny => 255,
		       regular => 65535,
		       medium => 16777215,
		       large => 4294967295 );

our (%INT_SIZE) = ( tiny => 'one byte integer',
		    small => 'two byte integer',
		    medium => 'three byte integer',
		    regular => 'four byte integer',
		    big => 'eight byte integer');

sub unpack_column_type {

    my ($cr, $typeinfo) = @_;

    if ( $typeinfo =~ qr{ ^ ( var )? ( char | binary ) [(] ( \d+ ) }xs )
    {
	my $type = $2 eq 'char' ? 'text' : 'data';
	my $mode = $1 ? 'variable' : 'fixed';
	my $size = $3;
	my ($charset) = $cr->{Collation} =~ qr{ ^ ([^_]+) }xs;
	$cr->{TypeParams} = [ $type, $size, $mode, $charset ];
    }
    
    elsif ( $typeinfo =~ qr{ ^ ( tiny | medium | long )? ( text | blob ) (?: [(] ( \d+ ) )? }xs )
    {
	my $type = $2 eq 'text' ? 'text' : 'data';
	my $size = $3 || $PREFIX_SIZE{$1 || 'regular'};
	my ($charset) = $cr->{Collation} =~ qr{ ^ ([^_]+) }xs;
	$cr->{TypeParams} = [ $type, $size, 'variable', $charset ];
    }
    
    elsif ( $type =~ qr{ ^ tinyint [(] 1 [)] }xs )
    {
	$cr->{TypeParams} = [ 'boolean' ];
    }
    
    elsif ( $typeinfo =~ qr{ ^ (tiny|small|medium|big)? int [(] (\d+) [)] \s* (unsigned)? }xs )
    {
	my $bound = $1 || 'regular';
	my $unsigned = $3 ? 'unsigned' : '';
	$cr->{TypeParams} = [ 'integer', $unsigned, $bound, $2 ];
    }
    
    elsif ( $typeinfo =~ qr{ ^ decimal [(] (\d+) , (\d+) [)] \s* (unsigned)? }xs )
    {
	my $unsigned = $3 ? 'unsigned' : '';
	my $before = $1 - $2;
	my $after = $2;
	
	$after = 10 if $after > 10;	# If people want fields with more than 10 decimals, they should
					# use floating point.
	
	$cr->{TypeParams} = [ 'fixed', $unsigned, $before, $after ];
    }
    
    elsif ( $typeinfo =~ qr{ ^ ( float | double ) (?: [(] ( \d+ ) , ( \d+ ) [)] )? \s* (unsigned)? }xs )
    {
	my $unsigned = $3 ? 'unsigned' : '';
	my $precision = $1;
	my $before = defined $2 ? $2 - $3 : undef;
	my $after = $3;
	$cr->{TypeParams} = [ 'floating', $unsigned, $precision, $before, $after ];
    }
    
    elsif ( $typeinfo =~ qr{ ^ bit [(] ( \d+ ) }xs )
    {
	$cr->{TypeParams} = [ 'bits', $1 ];
    }
    
    elsif ( $typeinfo =~ qr{ ^ ( enum | set ) [(] (.+) [)] $ }xs )
    {
	my $type = $1;
	my $list = $2;
	my $value_hash = unpack_enum($2);
	$cr->{TypeParams} = [ $type, $value_hash, $list ];
    }
    
    elsif ( $typeinfo =~ qr{ ^ ( date | time | datetime | timestamp ) \b }xs )
    {
	$cr->{TypeParams} = [ 'datetime', $1 ];
    }
    
    elsif ( $typeinfo =~ qr{ ^ ( (?: multi )? (?: point | linestring | polygon ) ) \b }xs )
    {
	$cr->{TypeParams} = [ 'geometry', $1 ];
    }
    
    elsif ( $typeinfo =~ qr{ ^ ( geometry (?: collection )? ) \b }xs )
    {
	$cr->{TypeParams} = [ 'geometry', $1 ];
    }
    
    else
    {
	$cr->{TypeParams} = [ 'unknown' ];
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


# get_column_property ( table_specifier, column_name, property )
# 
# Return the specified information from the schema cache, if it is available.

sub get_column_property {

    my ($edt, $table_specifier, $column_name, $property) = @_;
    
    croak "required arguments: table specifier, column name"
	unless $table_specifier && $column_name;

    my $app = $edt->app;
    
    if ( my $pcache = $COLUMN_SCHEMA_CACHE{$app}{$table_specifier}{$column_name} )
    {
	if ( $property )
	{
	    return defined $pcache->{$property} ? copy_property_value($pcache->{$property}) : undef;
	}
	
	else
	{
	    local $_;
	    return map { $_ => copy_property_value($pcache->{$_}) } keys $pcache->%*;
	}
    }
}


sub copy_property_value {

    if ( ref $_[0] && ( ref $_[0] eq 'ARRAY' || ref $_[0] eq 'HASH' ) )
    {
	if ( ref $_[0] eq 'HASH' )
	{
	    return { $_[0]->%* };
	}
	
	else
	{
	    return  = [ $_[0]->@* ];
	}
    }
    
    else
    {
	return $_[0];
    }
}


# alter_column_property ( table_specifier, column_name, property, value )
# 
# This routine is intended primarily for testing purposes. It alters a specific entry in the
# $COLUMN_SCHEMA_CACHE.

sub alter_column_property {
    
    my ($edt, $table_specifier, $column_name, $property, $value) = @_;
    
    croak "required arguments: table specifier, column name, property name"
	unless $table_specifier && $column_name && $property;
    
    my $app = $edt->app;
    
    if ( defined $COLUMN_SCHEMA_CACHE{$app}{$table_specifier} )
    {
	$COLUMN_SCHEMA_CACHE{$app}{$table_specifier}{$column_name}{$property} = $value;
	return 1;
    }
    
    else
    {
	return undef;
    }
}


# Data column validation routines
# -------------------------------

# validate_char_value ( type, fieldname, value )
# 
# Check that the specified value is suitable for storing into a boolean column in the
# database. If it is not, return an error condition as a listref.
# 
# If the value is good, return a canonical version suitable for storing into the column. An
# undefined return value will indicate a null. The second return value, if present, will be a
# warning condition as a listref. The third return value, if present, will indicate that the
# returned value has already been quoted.

sub validate_char_value {
    
    my ($edt, $cr, $value, $fieldname) = @_;

    my ($type, $size, $var, $charset) =
	ref $cr->{TypeParams} eq 'ARRAY' ? $cr->{TypeParams}->@* : $cr->{TypeParams} || 'unknown';
    
    my ($value_size, $no_quote, $additional);
    
    # If the character set of a text/char column is not utf8, then encode the value into the
    # proper character set before checking the length.
    
    if ( $type eq 'text' && $charset && $charset ne 'utf8' )
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
	    my $dbh = $edt->dbh;
	    my $quoted = $dbh->quote($value);
	    $value = "convert($quoted using $charset)";
	    ($value_size) = $dbh->selectrow_array("SELECT length($value)");
	    $no_quote = 1;
	}
    }
    
    else
    {
	$value_size = length(encode('UTF-8', $value));
    }
    
    # If the size of the value exceeds the size of the column, then we either truncate the data if
    # the column has the ALLOW_TRUNCATE attribute or else reject the value.
    
    if ( defined $size && $value_size > $size )
    {
	my $word = $type eq 'text' ? 'characters' : 'bytes';
	
	if ( $cr->{ALLOW_TRUNCATION} )
	{
	    $value = substr($value, 0, $size);
	    $additional = [ 'W_TRUNC', $fieldname,
			    "value was truncated to a length of $size $word" ];
	}
	
	else
	{
	    return [ 'E_WIDTH', $fieldname,
		     "value exceeds column size of $size $word, was $value_size" ];
	}
    }
    
    return ($value, $additional, $no_quote);
}


# validate_boolean_value ( type, fieldname, value )
# 
# Check that the specified value is suitable for storing into a boolean column in the
# database. If it is not, add an error condition and return a non-scalar value as a flag to
# indicate that no further processing should be done on it.
# 
# If the value is good, this routine will return a canonical version suitable for storing into the
# column. An undefined return value will indicate a null.

sub validate_boolean_value {
    
    my ($edt, $cr, $value, $fieldname) = @_;
    
    # For a boolean column, the value must be either 1 or 0. But we allow 'yes', 'no', 'true',
    # 'false', 'on', 'off' as case-insensitive synonyms. A string that is empty or has only
    # whitespace is turned into a null.
    
    if ( $value =~ qr{ ^ \s* $ }xs )
    {
	return undef;
    }
    
    elsif ( $value =~ qr{ ^ \s* (?: ( 1 | true | yes | on ) | 
				    ( 0 | false | no | off ) ) \s* $ }xsi )
    {
	return $1 ? '1' : '0';
    }
    
    else
    {
	return [ 'E_FORMAT', $fieldname, "value must be one of: 1, 0, true, false, yes, no, on, off" ];
    }
}


# validate_integer_value ( type, fieldnme, value )
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
    
    my ($edt, $cr, $value, $fieldname) = @_;
    
    my ($type, $unsigned, $size) =
	ref $cr->{TypeParams} eq 'ARRAY' ? $cr->{TypeParams}->@* : $cr->{TypeParams} || 'unknown';
    
    my $max = $unsigned ? $UNSIGNED_BOUND{$size} : $SIGNED_BOUND{$size};
    
    # First make sure that the value is either empty or matches the proper format. A value which
    # is empty or contains only whitespace will be treated as a NULL.
    
    if ( $value =~ qr{ ^ \s* $ }xs )
    {
	return undef;
    }
    
    elsif ( $value !~ qr{ ^ \s* ( [-+]? ) \s* ( \d+ ) \s* $ }xs )
    {
	my $phrase = $unsigned ? 'an unsigned' : 'an';
	
	return [ 'E_FORMAT', $fieldname, "value must be $phrase integer" ];
    }
    
    elsif ( $unsigned )
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
	
	else
	{
	    return $value;
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
	
	return $value; # otherwise
    }
}


# validate_fixed_value ( type, fieldname, value )
# 
# Check that the specified value is suitable for storing into a fixed-point decimal column in the
# database. If it is not, add an error condition and return a non-scalar value as a flag to
# indicate that no further processing should be done on it.
# 
# If the value is good, this routine will return a canonical version suitable for storing into the
# column. An undefined return value will indicate a null.

sub validate_fixed_value {

    my ($edt, $cr, $value, $fieldname) = @_;
    
    my ($type, $unsigned, $whole, $precision) =
	ref $cr->{TypeParams} eq 'ARRAY' ? $cr->{TypeParams}->@* : $cr->{TypeParams} || 'unknown';
    
    my $additional;
    
    # First make sure that the value is either empty or matches the proper format.  A value which
    # is empty or contains only whitespace is turned into NULL.
    
    if ( $value =~ qr{ ^ \s* $ }xs )
    {
	return undef;
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
	
	if ( $fracpart && length($fracpart) > $precision )
	{
	    my $total = $whole + $precision;
	    
	    if ( $cr->{ALLOW_TRUNCATION} )
	    {
		$additional = [ 'W_TRUNC', $fieldname,
				"value has been truncated to decimal($total,$precision)" ];
	    }
	    
	    else
	    {
		return [ 'E_WIDTH', $fieldname,
			 "too many decimal digits for decimal($total,$precision)" ];
	    }
	}
	
	# Rebuild the value, with the fracional part trimmed.
	
	$value = $sign;
	$value .= $intpart || '0';
	$value .= '.' . substr($fracpart, 0, $precision);
	
	return ($value, $additional);
    }
}


# validate_float_value ( type, fieldname, value )
# 
# Check that the specified value is suitable for storing into a floating-point decimal column in the
# database. If it is not, add an error condition and return a non-scalar value as a flag to
# indicate that no further processing should be done on it.
# 
# If the value is good, this routine will return a canonical version suitable for storing into the
# column. An undefined return value will indicate a null.

sub validate_float_value {

    my ($edt, $cr, $value, $fieldname) = @_;
    
    my ($type, $unsigned, $precision) = 
	ref $cr->{TypeParams} eq 'ARRAY' ? $cr->{TypeParams}->@* : $cr->{TypeParams} || 'unknown';
    
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
	
	$value = $sign . ( $2 // '' ) . '.';
	$value .= ( $3 // $4 // '' );
	
	if ( $6 )
	{
	    my $esign = $5 eq '-' ? '-' : '';
	    $value .= 'E' . $esign . $6;
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

	return $value;
    }
}


# validate_enum_value ( type, fieldname, value )
# 
# Check that the specified value is suitable for storing into an enumerated or set valued column in the
# database. If it is not, add an error condition and return a non-scalar value as a flag to
# indicate that no further processing should be done on it.
# 
# If the value is good, this routine will return a canonical version suitable for storing into the
# column. An undefined return value will indicate a null.

sub validate_enum_value {

    my ($edt, $cr, $value, $fieldname) = @_;
    
    my ($type, $good_values) =
	ref $cr->{TypeParams} eq 'ARRAY' ? $cr->{TypeParams}->@* : $cr->{TypeParams} || 'unknown';
    
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
	
	elsif ( $good_values->{fc $v} )
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
	return join(',', @good);
    }
    
    else
    {
	return undef;
    }
}


# validate_datetime_value ( type, fieldname, value )
# 
# Check that the specified value is suitable for storing into a time or date or datetime valued
# column in the database. If it is not, add an error condition and return a non-scalar value as a
# flag to indicate that no further processing should be done on it.
# 
# If the value is good, this routine will return a canonical version suitable for storing into the
# column. An undefined return value will indicate a null.

sub validate_datetime_value {
    
    my ($edt, $cr, $value, $fieldname) = @_;
    
    my ($type, $specific) = 
	ref $cr->{TypeParams} eq 'ARRAY' ? $cr->{TypeParams}->@* : $cr->{TypeParams} || 'unknown';
    
    if ( $value =~ qr{ ^ now (?: [(] [)] ) ? $ }xsi )
    {
	return ('NOW()', undef, 1);
    }
    
    elsif ( $value =~ qr{ ^ \d\d\d\d\d\d\d\d\d\d+ $ }xs )
    {
	return ("FROM_UNIXTIME($value)", undef, 1);
    }
    
    elsif ( $specific eq 'time' )
    {
	if ( $value !~ qr{ ^ \d\d : \d\d : \d\d $ }xs )
	{
	    return [ 'E_FORMAT', $fieldname, "invalid time '$value'" ];
	}

	else
	{
	    return $value;
	}
    }
    
    else
    {
	if ( $value !~ qr{ ^ ( \d\d\d\d - \d\d - \d\d ) ( \s+ \d\d : \d\d : \d\d ) ? $ }xs )
	{
	    return [ 'E_FORMAT', $fieldname, "invalid datetime '$value'" ];
	}
	
	unless ( defined $2 && $2 ne '' )
	{
	    $value .= ' 00:00:00';
	}
	
	return $value;
    }
}


# validate_geometry_value ( type, fieldname, value )
# 
# Check that the specified value is suitable for storing into a geometry valued column in the
# database. If it is not, add an error condition and return a non-scalar value as a flag to
# indicate that no further processing should be done on it.
# 
# If the value is good, this routine will return a canonical version suitable for storing into the
# column. An undefined return value will indicate a null.

sub validate_geometry_value {
    
    my ($edt, $cr, $value, $fieldname) = @_;
    
    my ($type, $specific) = 
	ref $cr->{TypeParams} eq 'ARRAY' ? $cr->{TypeParams}->@* : $cr->{TypeParams} || 'unknown';
    
    # $$$ we still need to write some code to validate these.
    
    return $value;
}


# Action execution routines
# -------------------------

sub execute_insert {


}


sub execute_replace {



}


sub execute_update {



}


sub execute_delete {



}

1;
